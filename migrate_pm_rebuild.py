#!/usr/bin/env python3
"""
One-time product_master rebuild — LABEL-BASED single source of truth (Option A).

Rebuilds product_master / needs_review / pm_overrides from the confirmed unified
label mapping (_unified_pm_mapping.json, 427 rows -> 77 label folders). Every
Meesho/Flipkart/Shopsy/Amazon listing lands in ONE label folder; no slug docs,
no fragmentation, no legacy junk.

SAFE BY DEFAULT: prints what it would do. Pass --apply to actually write.
PRE-REQ (enforced): a fresh _firestore_backup_*.json must exist (full export
before any delete). The scheduled pipeline workflow MUST be paused for the
wipe->load window (do not rely on timing).

Phases (in order, fail-safe):
  1. seed pm_overrides   (427 docs, keyed {platform}_{override_id}, incl target_design)
  2. wipe                (delete all product_master + needs_review docs; pm_overrides
                          replaced in phase 1 — old LEGACY_fk-* ones deleted too)
  3. load                (run the real processors against the known files with the
                          seeded overrides, write product_master via the pipeline's
                          own write_product_master_ids — same code path as a live run)
  4. reattach fk_url     (targeted field write from _fk_url_salvage.json; never
                          touches any other field)
  5. verify              (counts + folder check; expects 77 folders, 0 needs_review)
"""
import argparse, json, sys, io, glob
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
ME_CATALOG = BASE / 'processed' / '2026-05-23' / 'Catelog details.xlsx'
FK_LISTING = 'H:/My Drive/Rumee Raw Data/flipkart/listings/flipkart_listings_2026-07-02.xls'


def load_mapping():
    rows = json.load(open(BASE / '_unified_pm_mapping.json', encoding='utf-8'))
    ov = {}
    for x in rows:
        ov[f"{x['platform']}_{x['override_id']}"] = {
            'target_sku_id':          x['target_sku_id'],
            'target_variation_type':  x['variation'],
            'target_design':          x['design'],
        }
    return rows, ov


def ensure_backup(apply):
    """A full backup MUST exist before any wipe. Use an existing local export if
    present, else take a fresh one via the Admin SDK (Actions path). The workflow
    uploads the resulting file as a build artifact."""
    b = sorted(glob.glob(str(BASE / '_firestore_backup_*.json')))
    if b:
        print(f"  backup present: {Path(b[-1]).name}")
        return
    if not apply:
        print("  (dry) no local backup; --apply would take a fresh one via Admin SDK")
        return
    from firestore_connector import get_db
    from datetime import datetime
    db = get_db()
    out = {'exported_at': datetime.now().isoformat()}
    for col in ('product_master', 'needs_review', 'pm_overrides'):
        out[col] = [dict(s.to_dict() or {}, _id=s.id) for s in db.collection(col).get()]
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = BASE / f'_firestore_backup_{ts}.json'
    json.dump(out, open(path, 'w', encoding='utf-8'), ensure_ascii=False, indent=1, default=str)
    print(f"  backup taken: {path.name} "
          f"(product_master={len(out['product_master'])}, "
          f"needs_review={len(out['needs_review'])}, pm_overrides={len(out['pm_overrides'])})")


def phase_seed(rows, apply):
    from firestore_connector import get_db
    print(f"\n[1] seed pm_overrides — {len(rows)} docs")
    if not apply:
        print("    (dry) would delete all existing pm_overrides then write", len(rows)); return
    db = get_db()
    # delete existing (incl legacy LEGACY_fk-*), then write fresh
    for snap in db.collection('pm_overrides').get():
        db.collection('pm_overrides').document(snap.id).delete()
    batch = db.batch(); n = 0
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    for x in rows:
        doc_id = f"{x['platform']}_{x['override_id']}"
        batch.set(db.collection('pm_overrides').document(doc_id), {
            'platform':               x['platform'],
            'catalog_id':             x['catalog_id'],
            'override_id':            x['override_id'],
            'raw_sku':                x['raw_sku'],
            'target_sku_id':          x['target_sku_id'],
            'target_variation_type':  x['variation'],
            'target_design':          x['design'],
            'assigned_by':            'bulk_import_2026-07-03',
            'assigned_at':            now,
        })
        n += 1
        if n % 450 == 0:
            batch.commit(); batch = db.batch()
    if n % 450:
        batch.commit()
    print(f"    wrote {n} pm_overrides")


def phase_wipe(apply):
    print("\n[2] wipe product_master + needs_review")
    try:
        from firestore_connector import get_db
        db = get_db()
    except Exception as e:
        print(f"    (dry — Firestore not reachable locally: {e}); would delete both collections")
        return
    for col in ('product_master', 'needs_review'):
        ids = [s.id for s in db.collection(col).get()]
        print(f"    {col}: {len(ids)} docs" + ("" if apply else " (dry — not deleted)"))
        if apply:
            for did in ids:
                db.collection(col).document(did).delete()


def phase_load(ov, apply):
    import process
    print("\n[3] load — build product_master from processors + seeded overrides")
    me_cat, _, me_nr = process.process_catalog(ME_CATALOG, pm_overrides=ov)
    _, _, fk_nr, fk_cat = process.process_fk_listings(FK_LISTING, pm_overrides=ov)
    pm = {}
    for src in (me_cat, fk_cat):
        for sid, e in src.items():
            if sid in pm:
                seen = {(l.get('product_id') or l.get('catalog_id')) for l in pm[sid]['listings']}
                for l in e['listings']:
                    k = l.get('product_id') or l.get('catalog_id')
                    if k not in seen:
                        pm[sid]['listings'].append(l); seen.add(k)
            else:
                pm[sid] = dict(e, listings=list(e['listings']))
    leftovers = me_nr + fk_nr
    print(f"    folders (Meesho+FK+Shopsy): {len(pm)} | needs_review leftovers: {len(leftovers)}")
    if leftovers:
        print("    WARNING: unexpected needs_review during load — investigate before apply:")
        for x in leftovers[:20]:
            print("      ", x.get('platform'), x.get('raw_sku'), x.get('catalog_id'))
    if apply:
        from firestore_connector import write_product_master_ids
        write_product_master_ids(pm)
        # Amazon path reads Firestore rumee_az_catalog directly
        az_listings, az_nr = process.process_az_catalog_for_pm(pm_overrides=ov)
        if az_listings:
            from firestore_connector import write_az_product_master
            write_az_product_master(az_listings)
        if leftovers or az_nr:
            from firestore_connector import write_needs_review
            write_needs_review(leftovers + az_nr)
    return pm


def phase_cleanup_orphans(apply):
    """Delete malformed product_master docs that contain ONLY fk_url (no
    listings) — created by running reattach before the pipeline rebuilt the
    real docs. Only deletes docs matching the exact salvage folder list AND
    lacking a listings array, so a real rebuilt doc is never touched."""
    import re
    from firestore_connector import get_db
    sal = json.load(open(BASE / '_fk_url_salvage.json', encoding='utf-8'))
    print(f"\n[cleanup] checking {len(sal)} salvage folder doc-ids for orphan stubs")
    db = get_db()
    deleted = 0
    for folder in sal:
        doc_id = re.sub(r'[/. ]', '_', folder)
        snap = db.collection('product_master').document(doc_id).get()
        if not snap.exists:
            continue
        d = snap.to_dict() or {}
        if d.get('listings'):
            print(f"    SKIP (has listings, not an orphan): {doc_id}")
            continue
        print(f"    orphan: {doc_id} — fields: {list(d.keys())}")
        if apply:
            db.collection('product_master').document(doc_id).delete()
            deleted += 1
    print(f"    {'deleted' if apply else 'would delete'} {deleted if apply else '(see above)'} orphan docs")


def phase_reattach(apply):
    from firestore_connector import get_db
    import re
    sal = json.load(open(BASE / '_fk_url_salvage.json', encoding='utf-8'))
    print(f"\n[4] reattach fk_url — {len(sal)} folders")
    if not apply:
        print("    (dry) would patch fk_url on", len(sal), "docs"); return
    db = get_db()
    patched, missing = 0, []
    for folder, url in sal.items():
        doc_id = re.sub(r'[/. ]', '_', folder)
        # Only patch a doc that ALREADY EXISTS with real listings — merge=True
        # on a non-existent doc CREATES it, which produced 30 malformed
        # fk_url-only stub docs (design/variation_type/listings missing) when
        # this ran before the pipeline had rebuilt product_master. Never again.
        snap = db.collection('product_master').document(doc_id).get()
        if not snap.exists or not (snap.to_dict() or {}).get('listings'):
            missing.append(doc_id); continue
        db.collection('product_master').document(doc_id).set({'fk_url': url}, merge=True)
        patched += 1
    print(f"    patched {patched} fk_url values"
          + (f" | SKIPPED (doc not yet rebuilt): {missing}" if missing else ""))


def phase_verify(apply):
    from firestore_connector import get_db
    print("\n[5] verify")
    db = get_db()
    pm = list(db.collection('product_master').get())
    nr = list(db.collection('needs_review').get())
    print(f"    product_master docs: {len(pm)} (expect ~77) | needs_review: {len(nr)} (expect 0)")
    slugish = [s.id for s in pm if s.id.endswith('-me') or s.id in ('dj7b', 'dj5b', 'bangle')]
    print(f"    slug-style doc ids: {len(slugish)} {slugish[:10]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='actually write to Firestore')
    ap.add_argument('--no-load', action='store_true',
                    help='skip the local-file product_master build (Actions path — '
                         'the normal Drive-reading pipeline run rebuilds it instead)')
    ap.add_argument('--reattach', action='store_true',
                    help='run the fk_url reattach phase — ONLY after product_master has '
                         'already been rebuilt (by a normal pipeline run) with real listings')
    ap.add_argument('--cleanup-orphans', action='store_true',
                    help='delete malformed product_master docs that contain only fk_url '
                         'and no listings (created by a premature reattach)')
    a = ap.parse_args()

    if a.cleanup_orphans:
        print("=== cleanup orphan fk_url stub docs ===", "APPLY" if a.apply else "DRY RUN")
        phase_cleanup_orphans(a.apply)
        return

    print("=== product_master rebuild (Option A label-based) ===",
          "APPLY" if a.apply else "DRY RUN", "(no-load)" if a.no_load else "")
    ensure_backup(a.apply)
    rows, ov = load_mapping()
    phase_seed(rows, a.apply)
    phase_wipe(a.apply)
    if not a.no_load:
        phase_load(ov, a.apply)
    if a.reattach:
        # Safe even if run in the same pass as phase_load: phase_reattach only
        # patches docs that already have listings, never creates stubs.
        phase_reattach(a.apply)
    if a.apply and not a.no_load:
        phase_verify(a.apply)
    print("\nDONE" + ("" if a.apply else " — dry run, nothing written. Re-run with --apply after review + cron pause."))


if __name__ == '__main__':
    main()
