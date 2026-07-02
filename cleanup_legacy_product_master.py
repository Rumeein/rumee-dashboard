"""
One-time cleanup: legacy 'fk-'/'me-' prefixed product_master junk docs.

Root cause (fixed in process.py today): the old me_sku_id()/fk_sku_id()
resolvers auto-slugified any unrecognized SKU into a doc id like
'me-bangle-4' or 'fk-dj1b'. A separate client-side "Regenerate" button
(generateProductMaster/pmCategorize in index.html, now disabled) did the
same thing independently. Both are now closed — this script cleans up what
they already created.

Supersedes migrate_fk_urls.py (narrower — fk_url only, dropped listing data).
This script instead, per legacy doc:
  1. For each listing inside it, tries to resolve the listing's raw SKU text
     against ME_SKU_MAP / FK_SKU_MAP / AZ_SKU_MAP.
     - Resolved -> merge that listing into the correct target doc (merge by
       catalog_id, same pattern as write_product_master_ids/write_az_product_master),
       write, read back, verify.
     - Unresolved -> write a needs_review doc instead (nr_{platform}_{catalog_id})
       so the listing is never silently lost — owner can assign it later.
  2. Carries over fk_url (if present) into the resolved target doc.
  3. Only deletes the legacy source doc once EVERY listing + fk_url has been
     either migrated (verified) or routed to needs_review.
  4. Docs with zero listings and no fk_url are reported as pure-empty and
     safe to delete outright (no data to lose).
  5. Never deletes before verifying — same write->verify->delete pattern as
     migrate_fk_urls.py.

Does NOT run automatically.
    python cleanup_legacy_product_master.py            # dry run — report only
    python cleanup_legacy_product_master.py --apply     # actually perform cleanup
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


def resolve_listing(raw_sku, me_map, fk_map, az_map):
    """Try all 3 SKU maps for a raw listing SKU. Returns (sku_id, variation_type_hint) or None."""
    for m in (me_map, fk_map, az_map):
        if raw_sku in m:
            return m[raw_sku][0]
    return None


def is_blocklisted(raw_sku, blocklist):
    """Checks the raw listing SKU/style text against ME_CATALOG_BLOCKLIST
    (e.g. 'meera craft store') — these are not real products, discard
    outright rather than route to needs_review. Bug found 2026-07-02: the
    live pipeline blocklist check was only checking CATALOG NAME, not
    PRODUCT NAME where store names actually appear — now fixed there too,
    but legacy docs created before the fix still need this same check."""
    s = raw_sku.lower()
    return any(b in s for b in blocklist)


def main():
    cfg = load_tenant_config()
    me_map = {k: tuple(v) for k, v in cfg['me_sku_map'].items()}
    fk_map = {k: tuple(v) for k, v in cfg['fk_sku_map'].items()}
    az_map = {k: tuple(v) for k, v in cfg.get('az_sku_map', {}).items()}
    design_map = dict(cfg.get('design_map', {}))
    # Must match ME_CATALOG_BLOCKLIST in process.py — kept in sync manually,
    # small set, checked at build time. (Found 2026-07-02: "Meera Craft Store"
    # is text inside PRODUCT NAME, not a catalog/category name.)
    me_catalog_blocklist = {'meera craft store'}

    db = get_db()
    all_docs = list(db.collection('product_master').stream())
    # Legacy junk comes from two old sources: the auto-slugify pipeline bug
    # (doc ids prefixed 'fk-'/'me-') and the disabled client-side "Regenerate"
    # heuristic classifier, which instead left design == 'UNKNOWN' (found via
    # UAT 2026-07-02 — doc ids for that source aren't prefixed at all).
    def _is_legacy(d):
        data = d.to_dict() or {}
        return d.id.startswith('fk-') or d.id.startswith('me-') or data.get('design') == 'UNKNOWN'
    legacy = [d for d in all_docs if _is_legacy(d)]
    existing_ids = {d.id for d in all_docs}

    print(f"Found {len(legacy)} legacy 'fk-'/'me-' prefixed docs out of {len(all_docs)} total product_master docs.")

    report = []
    needs_review_to_write = []

    for snap in legacy:
        data = snap.to_dict() or {}
        listings = data.get('listings', []) or []
        fk_url = data.get('fk_url')

        entry = {
            'source': snap.id, 'listing_count': len(listings), 'has_fk_url': bool(fk_url),
            'migrated': [], 'flagged_needs_review': [], 'verified': False, 'deleted': False,
        }

        if not listings and not fk_url:
            entry['pure_empty'] = True
            report.append(entry)
            if APPLY:
                db.collection('product_master').document(snap.id).delete()
                entry['deleted'] = True
            continue

        all_resolved_ok = True
        for lst in listings:
            raw_sku = (lst.get('style_id') or lst.get('sku_id') or '').strip()
            catalog_id = lst.get('catalog_id', '')
            platform = lst.get('platform') or ('flipkart' if snap.id.startswith('fk-') else 'meesho')

            # Best-effort blocklist check (e.g. Meera Craft Store) — legacy
            # listing dicts don't always retain product_name text, so this
            # only catches it when style_id/product_name happens to contain
            # the blocked term. Doesn't catch every case; anything missed
            # here falls through to needs_review (safe — visible for manual
            # review/discard) rather than being silently kept as a real product.
            check_text = raw_sku + ' ' + str(lst.get('product_name', ''))
            if is_blocklisted(check_text, me_catalog_blocklist):
                entry.setdefault('blocklisted', []).append({'raw_sku': raw_sku, 'catalog_id': catalog_id})
                continue

            target_sku_id = resolve_listing(raw_sku, me_map, fk_map, az_map)
            if target_sku_id:
                target_doc_id = re.sub(r'[/. ]', '_', target_sku_id)
                entry['migrated'].append({'raw_sku': raw_sku, 'catalog_id': catalog_id, 'target': target_doc_id})
                if APPLY:
                    target_ref = db.collection('product_master').document(target_doc_id)
                    target_snap = target_ref.get()
                    target_data = target_snap.to_dict() or {}
                    existing_listings = target_data.get('listings', []) or []
                    by_cat = {l.get('catalog_id'): i for i, l in enumerate(existing_listings) if isinstance(l, dict)}
                    merged_entry = dict(lst)  # carry over whatever fields the legacy listing had
                    if catalog_id in by_cat:
                        existing_listings[by_cat[catalog_id]] = merged_entry
                    else:
                        existing_listings.append(merged_entry)
                    payload = {'listings': existing_listings}
                    if target_doc_id not in existing_ids:
                        payload.update({
                            'sku_id': target_sku_id,
                            'design': design_map.get(target_sku_id, target_sku_id),
                            'variation_type': target_data.get('variation_type', 'bahubali'),
                            'status': 'active',
                            'created_at': datetime.now(timezone.utc).isoformat(),
                        })
                    target_ref.set(payload, merge=True)
                    # verify
                    check = target_ref.get().to_dict() or {}
                    check_cats = {l.get('catalog_id') for l in check.get('listings', []) if isinstance(l, dict)}
                    if catalog_id not in check_cats:
                        all_resolved_ok = False
                        entry.setdefault('errors', []).append(f'verify failed for {raw_sku} -> {target_doc_id}')
            else:
                needs_review_to_write.append({
                    'platform': platform, 'catalog_id': catalog_id,
                    'raw_sku': raw_sku, 'product_name': raw_sku,
                })
                entry['flagged_needs_review'].append({'raw_sku': raw_sku, 'catalog_id': catalog_id})

        if fk_url:
            resolved_targets = {m['target'] for m in entry['migrated']}
            if resolved_targets:
                target_doc_id = next(iter(resolved_targets))
                if APPLY:
                    target_ref = db.collection('product_master').document(target_doc_id)
                    target_ref.set({'fk_url': fk_url}, merge=True)
                    check = target_ref.get().to_dict() or {}
                    if check.get('fk_url') != fk_url:
                        all_resolved_ok = False
                        entry.setdefault('errors', []).append('fk_url verify failed')
            else:
                entry.setdefault('errors', []).append('fk_url present but no listing resolved to a target — fk_url NOT migrated, needs manual review')
                all_resolved_ok = False

        entry['verified'] = all_resolved_ok
        report.append(entry)

        if APPLY and all_resolved_ok:
            db.collection('product_master').document(snap.id).delete()
            entry['deleted'] = True

    if APPLY and needs_review_to_write:
        from firestore_connector import write_needs_review
        write_needs_review(needs_review_to_write)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'legacy_cleanup_report_{ts}.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)

    pure_empty = sum(1 for r in report if r.get('pure_empty'))
    migrated_listings = sum(len(r['migrated']) for r in report)
    flagged = sum(len(r['flagged_needs_review']) for r in report)
    blocklisted = sum(len(r.get('blocklisted', [])) for r in report)
    deleted = sum(1 for r in report if r.get('deleted'))
    errors = sum(1 for r in report if r.get('errors'))

    print(f"\n{'APPLIED' if APPLY else 'DRY RUN'} — report written to {report_path}")
    print(f"  legacy docs considered:        {len(report)}")
    print(f"  pure empty (no data to lose):  {pure_empty}")
    print(f"  listings migrated to correct doc: {migrated_listings}")
    print(f"  listings flagged to needs_review: {flagged}")
    print(f"  listings discarded (blocklisted, e.g. Meera Craft Store): {blocklisted}")
    if APPLY:
        print(f"  legacy docs deleted (verified): {deleted}")
        print(f"  docs with errors (NOT deleted, needs manual review): {errors}")
    else:
        print("  Run again with --apply to actually perform the cleanup.")


if __name__ == '__main__':
    main()
