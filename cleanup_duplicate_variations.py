"""
One-time cleanup: product_master docs that share the same (design,
variation_type) label under two or more different sku_ids.

Root cause (fixed in firestore_connector.write_product_master_ids() and
index.html's pmWrite('reassign_variation'/'assign') today): sku_id (the doc
id) came from an arbitrary label folder chosen at Assign/Reassign time, not
derived from (design, variation_type). Two different actions could pick two
different sku_ids for what a human considers the same variation, so the
Products tab ends up showing e.g. "Bahubali Chain / Base" as two separate
rows with different listings under each. The write-path fix stops this going
forward; this script cleans up duplicates that already exist.

Per this project's established pattern for any product_master mutation
(see cleanup_legacy_product_master.py): dry run by default, only writes and
deletes on --apply, and never deletes a doc before its listings have been
verified as present on the canonical doc.

IMPORTANT — do not run --apply while a pipeline run may be in flight. This
script reads all product_master docs once up front, then writes/deletes
later; a concurrent pipeline write to one of the "other" docs in between
would be silently lost when that doc is deleted, and a concurrent pipeline
write to an already-deleted doc would recreate it (merge=True on a missing
doc just creates it). Run --apply manually, well outside the pipeline's
schedule window.

    python cleanup_duplicate_variations.py            # dry run — report only
    python cleanup_duplicate_variations.py --apply     # actually perform cleanup
"""
import json
import os
import sys
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import credentials, firestore

APPLY = '--apply' in sys.argv


def get_db():
    cred_json = os.environ.get('FIREBASE_CREDENTIALS')
    if not cred_json:
        raise SystemExit("FIREBASE_CREDENTIALS env var not set")
    cred = credentials.Certificate(json.loads(cred_json))
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    return firestore.client()


def _pm_folder_name(design, variation):
    """Mirrors index.html's _pmFolderName() and the same helper added to
    write_product_master_ids() — Base/empty variation collapses to design
    only, design==variation collapses to design only. Keeping this script's
    grouping consistent with the write-path fix means it catches exactly the
    same duplicates that fix now prevents, not a narrower set."""
    l1 = str(design or '').strip()
    l2 = str(variation or '').strip()
    if not l2 or l2.lower() == 'base':
        return l1
    if l1.lower() == l2.lower():
        return l1
    return (l1 + ' ' + l2).strip()


def _norm_label(design, vtype):
    return _pm_folder_name(design, vtype).lower()


def _mkey(listing):
    """Same per-listing identity key used everywhere else (product_id, else
    catalog_id) — keeps this script's dedup consistent with the pipeline and
    the dashboard's own merge logic."""
    if not isinstance(listing, dict):
        return ''
    return str(listing.get('product_id') or listing.get('catalog_id') or '')


def pick_canonical(docs):
    """Prefer the doc with the most listings (most real data); tie-break by
    earliest created_at (oldest = the original), then by doc id for a stable
    result when neither signal is available."""
    def sort_key(d):
        data = d['data']
        listing_count = len(data.get('listings') or [])
        created_at = data.get('created_at') or ''
        return (-listing_count, created_at or '9999', d['id'])
    return sorted(docs, key=sort_key)[0]


def main():
    db = get_db()
    all_docs = [{'id': snap.id, 'data': snap.to_dict() or {}} for snap in db.collection('product_master').stream()]
    print(f"Loaded {len(all_docs)} product_master docs.")

    groups = {}
    for d in all_docs:
        label = _norm_label(d['data'].get('design'), d['data'].get('variation_type'))
        groups.setdefault(label, []).append(d)

    dup_groups = {label: docs for label, docs in groups.items() if len(docs) > 1}
    print(f"Found {len(dup_groups)} (design, variation_type) labels with 2+ docs.")

    report = []
    total_merged_listings = 0
    total_deleted = 0

    for label, docs in dup_groups.items():
        canonical = pick_canonical(docs)
        others = [d for d in docs if d['id'] != canonical['id']]

        entry = {
            'label': label,
            'canonical_doc': canonical['id'],
            'canonical_listing_count_before': len(canonical['data'].get('listings') or []),
            'merged_from': [], 'errors': [], 'deleted': [],
        }

        merged_listings = list(canonical['data'].get('listings') or [])
        by_key = {_mkey(l): i for i, l in enumerate(merged_listings) if _mkey(l)}

        for other in others:
            other_listings = other['data'].get('listings') or []
            added, updated = 0, 0
            for lst in other_listings:
                k = _mkey(lst)
                if not k:
                    continue
                if k in by_key:
                    updated += 1
                    continue  # canonical already has this real listing — don't clobber it
                merged_listings.append(lst)
                by_key[k] = len(merged_listings) - 1
                added += 1
            entry['merged_from'].append({
                'doc_id': other['id'], 'listing_count': len(other_listings),
                'added': added, 'already_present': updated,
            })
            total_merged_listings += added

        entry['canonical_listing_count_after'] = len(merged_listings)
        report.append(entry)

        if not APPLY:
            continue

        canonical_ref = db.collection('product_master').document(canonical['id'])
        canonical_ref.set({'listings': merged_listings}, merge=True)
        check = canonical_ref.get().to_dict() or {}
        check_keys = {_mkey(l) for l in (check.get('listings') or [])}
        if not by_key.keys() <= check_keys:
            entry['errors'].append('verify failed: not all merged listing keys present after write')
            continue  # do NOT delete the others if the merge write didn't verify

        for other in others:
            db.collection('product_master').document(other['id']).delete()
            entry['deleted'].append(other['id'])
            total_deleted += 1

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'duplicate_variation_cleanup_report_{ts}.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n{'APPLIED' if APPLY else 'DRY RUN'} — report written to {report_path}")
    print(f"  duplicate-label groups found: {len(dup_groups)}")
    print(f"  listings merged into canonical docs: {total_merged_listings}")
    if APPLY:
        print(f"  extra docs deleted (verified): {total_deleted}")
        errors = sum(1 for r in report if r.get('errors'))
        print(f"  groups with errors (NOT deleted, needs manual review): {errors}")
    else:
        print("  Review the report, then run again with --apply to actually perform the cleanup.")


if __name__ == '__main__':
    main()
