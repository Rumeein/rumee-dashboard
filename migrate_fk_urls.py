"""
SUPERSEDED 2026-07-02 by cleanup_legacy_product_master.py — that script does
everything this one does (fk_url migration, write-verify-delete) PLUS also
migrates the legacy docs' actual listing data (not just fk_url) and covers
'me-' prefixed docs too, routing anything unresolvable to needs_review
instead of silently dropping it. Use cleanup_legacy_product_master.py instead.
Kept here for reference only — do not run.

One-time migration: extract fk_url from legacy 'fk-' prefixed product_master
junk docs (created by the old auto-slugify bug) and write it into the correct
variation-level doc, then delete the junk doc — but only after verifying the
write landed.

Safety pattern (per DOCS.md Sec 22 decision "migration script must verify
before delete"):
  1. write fk_url to the target doc (targeted field update only)
  2. read the target doc back
  3. confirm fk_url matches what was written
  4. only then delete the source 'fk-' doc

Does NOT guess a target when the mapping is ambiguous — those are written to
the report as 'ambiguous' for manual review instead.

This script does NOT run automatically. Review the report, then run manually:
    python migrate_fk_urls.py            # dry run — writes report, no Firestore writes
    python migrate_fk_urls.py --apply     # actually performs the migration
"""
import json
import os
import re
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


def load_tenant_config():
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tenant_config.json')
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def find_target_sku_id(legacy_doc_id, legacy_data, fk_sku_map, design_map):
    """
    Try to derive the correct target product_master doc id for a legacy
    'fk-{slug}' doc. Looks at the doc's own stored fields first (sku name,
    design), then tries matching the slug against FK_SKU_MAP values.
    Returns (target_sku_id, confidence) or (None, 'ambiguous') if no safe
    match can be derived from data alone.
    """
    # If the legacy doc happens to store the original raw SKU text anywhere
    # (design, sku_id fields, or listings[].style_id), try resolving it
    # through FK_SKU_MAP directly — the highest-confidence path.
    candidates = set()
    for field in ('design', 'sku_id'):
        v = legacy_data.get(field)
        if v:
            candidates.add(str(v).strip())
    for lst in legacy_data.get('listings', []) or []:
        if isinstance(lst, dict) and lst.get('style_id'):
            candidates.add(str(lst['style_id']).strip())

    for raw in candidates:
        if raw in fk_sku_map:
            return fk_sku_map[raw][0], 'high'

    # Fallback: slug-match against FK_SKU_MAP target sku_ids directly
    # (legacy doc id is 'fk-{slug}' — compare slug to mapped sku_ids)
    slug = legacy_doc_id[3:] if legacy_doc_id.startswith('fk-') else legacy_doc_id
    for raw, (sid, _display) in fk_sku_map.items():
        if sid == slug or sid.replace('-', '') == slug.replace('-', ''):
            return sid, 'medium'

    return None, 'ambiguous'


def main():
    cfg = load_tenant_config()
    fk_sku_map = {k: tuple(v) for k, v in cfg['fk_sku_map'].items()}
    design_map = dict(cfg.get('design_map', {}))

    db = get_db()
    docs = list(db.collection('product_master').stream())
    legacy = [d for d in docs if d.id.startswith('fk-')]
    existing_ids = {d.id for d in docs}

    print(f"Found {len(legacy)} legacy 'fk-' prefixed docs out of {len(docs)} total product_master docs.")

    report = []
    for snap in legacy:
        data = snap.to_dict() or {}
        fk_url = data.get('fk_url')
        if not fk_url:
            report.append({'source': snap.id, 'skipped': True, 'reason': 'no fk_url to migrate'})
            continue

        target_sku_id, confidence = find_target_sku_id(snap.id, data, fk_sku_map, design_map)
        if not target_sku_id:
            report.append({
                'source': snap.id, 'fk_url': fk_url, 'ambiguous': True,
                'reason': 'could not derive a target doc from stored data — needs manual review',
                'source_data': data,
            })
            continue

        target_doc_id = re.sub(r'[/. ]', '_', target_sku_id)
        entry = {
            'source': snap.id, 'target': target_doc_id, 'fk_url': fk_url,
            'confidence': confidence, 'verified': False, 'deleted': False,
        }

        if target_doc_id not in existing_ids:
            entry['note'] = 'target doc does not exist yet — will be created by write'

        if APPLY:
            target_ref = db.collection('product_master').document(target_doc_id)
            target_ref.set({'fk_url': fk_url}, merge=True)

            # verify before delete
            check = target_ref.get()
            check_data = check.to_dict() or {}
            if check_data.get('fk_url') == fk_url:
                entry['verified'] = True
                db.collection('product_master').document(snap.id).delete()
                entry['deleted'] = True
            else:
                entry['verified'] = False
                entry['deleted'] = False
                entry['error'] = 'read-back did not match written value — source doc NOT deleted'

        report.append(entry)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'fk_url_migration_report_{ts}.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)

    total = len(report)
    ambiguous = sum(1 for r in report if r.get('ambiguous'))
    skipped = sum(1 for r in report if r.get('skipped'))
    succeeded = sum(1 for r in report if r.get('deleted'))
    failed = sum(1 for r in report if APPLY and not r.get('deleted') and not r.get('ambiguous') and not r.get('skipped'))

    print(f"\n{'APPLIED' if APPLY else 'DRY RUN'} — report written to {report_path}")
    print(f"  total legacy docs considered: {total}")
    print(f"  skipped (no fk_url):          {skipped}")
    print(f"  ambiguous (manual review):    {ambiguous}")
    if APPLY:
        print(f"  succeeded (verified+deleted): {succeeded}")
        print(f"  failed (verify mismatch):     {failed}")
    else:
        print("  Run again with --apply to actually perform the migration.")


if __name__ == '__main__':
    main()
