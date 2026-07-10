#!/usr/bin/env python3
"""
One-time product_master cleanup — applies Jaiswal's corrections, originally
reviewed by hand in product_master_cleanup_2026-07-09.xlsx (Duplicate
Listings + Fragmented Designs tabs) and confirmed in chat 2026-07-10.
The corrections themselves are frozen into _pm_cleanup_corrections.json
(platform, catalog_id, design, variation only — no stock counts or raw SKU
text committed to this public repo, same minimal-data convention already
used for _unified_pm_mapping.json/_fk_url_salvage.json elsewhere in this repo).

Scope is narrow and explicit: only listings named in that JSON are touched.
Everything else in product_master is left exactly as it is (this is NOT a
rebuild).

Listings are moved (if needed) into the doc matching their corrected
(design, variation) via the SAME folder-naming rule as the pipeline/dashboard
(_pmFolderName / folder()): Base/empty variation -> design only; design ==
variation -> design only; else "design variation". A doc that ends up with
zero listings after this is deleted (empty product_master doc is nothing to
show, matching the existing convention elsewhere in this codebase).

SAFE BY DEFAULT: prints exactly what would change. Pass --apply to write.
Requires FIREBASE_CREDENTIALS (Admin SDK) — public client rules forbid the
create/delete/shrink operations this cleanup needs, so this must run
somewhere with the Admin SDK (GitHub Actions), not from a local machine
without the service account key.

PRE-REQ (enforced): a fresh _firestore_backup_*.json must exist before --apply.
"""
import argparse, json, re, sys, io, glob
from pathlib import Path
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
CORRECTIONS_JSON = BASE / '_pm_cleanup_corrections.json'


def folder_name(design, variation):
    l1 = str(design or '').strip()
    l2 = str(variation or '').strip()
    if not l2 or l2.lower() == 'base':
        return l1
    if l1.lower() == l2.lower():
        return l1
    return f'{l1} {l2}'


def doc_id_for(design, variation):
    return re.sub(r'[/. ]', '_', folder_name(design, variation))


def mkey(platform, product_id, catalog_id):
    # Matches by catalog_id, NOT product_id — the Excel (product_master_cleanup)
    # displays catalog_id (see build_cleanup_xlsx.py: "catalog_id or product_id",
    # catalog_id-first for display), so the correction data keys off catalog_id.
    # This differs from the pipeline's own internal merge key (product_id
    # preferred for Meesho) — found + fixed during dry-run review 2026-07-10:
    # a first version of this script keyed off product_id and silently failed
    # to match several Meesho listings whose Excel-displayed ID was catalog_id
    # while the live listing's product_id differed. catalog_id is present and
    # unique-per-listing for every row in these two sheets, so it's a safe key
    # for this narrow, human-reviewed cleanup (unlike the pipeline's general
    # multi-product-per-catalog_id Meesho case, not relevant here).
    return (platform, str(catalog_id) if catalog_id else str(product_id or ''))


def load_corrections():
    rows = json.load(open(CORRECTIONS_JSON, encoding='utf-8'))
    final_target = {}   # mkey -> (design, variation, source_note)
    for x in rows:
        k = mkey(x['platform'], None, x['catalog_id'])
        final_target[k] = (x['design'], x['variation'], x.get('source', ''))
    return final_target


def load_live_snapshot():
    """Try Admin SDK first (Actions path). Falls back to the cached local
    export for a LOCAL PREVIEW dry-run only — never used for --apply."""
    try:
        from firestore_connector import get_db
        db = get_db()
        docs = {}
        for snap in db.collection('product_master').get():
            docs[snap.id] = snap.to_dict() or {}
        return docs, True
    except Exception as e:
        cache = sorted(glob.glob(str(BASE / '_pm_snapshot_cache.json')))
        scratch = Path(r"C:\Users\jaisw\AppData\Local\Temp\claude\D--Claude-RuMee-Dashbord\aae3d354-412e-48d7-96ef-ab261c445454\scratchpad\pm.json")
        src = cache[-1] if cache else (scratch if scratch.exists() else None)
        if not src:
            raise RuntimeError(f"No live Firestore ({e}) and no local snapshot cache found") from e
        raw = json.load(open(src, encoding='utf-8'))
        docs = {}
        def fs_to_py(v):
            if 'stringValue' in v: return v['stringValue']
            if 'integerValue' in v: return int(v['integerValue'])
            if 'booleanValue' in v: return v['booleanValue']
            if 'nullValue' in v: return None
            if 'arrayValue' in v: return [fs_to_py(x) for x in v['arrayValue'].get('values', [])]
            if 'mapValue' in v: return {k: fs_to_py(x) for k, x in v['mapValue'].get('fields', {}).items()}
            return None
        for d in raw['documents']:
            doc_id = d['name'].split('/')[-1]
            docs[doc_id] = {k: fs_to_py(v) for k, v in d.get('fields', {}).items()}
        print(f"  (using LOCAL SNAPSHOT {Path(src).name} for dry-run preview — not live Firestore)")
        return docs, False


def build_plan(final_target, existing):
    """Returns (new_state: {doc_id: {design, variation_type, listings}},
    docs_to_delete: [doc_id])."""
    def lkey(l):
        return mkey(l.get('platform'), l.get('product_id'), l.get('catalog_id'))

    new_state = {}
    for doc_id, d in existing.items():
        new_state[doc_id] = {
            'design': d.get('design'), 'variation_type': d.get('variation_type'),
            'listings': list(d.get('listings') or []),
        }

    moves = []  # (mkey, listing, from_doc, to_doc)
    for doc_id, d in existing.items():
        for l in (d.get('listings') or []):
            k = lkey(l)
            if k not in final_target:
                continue
            design, variation, _src = final_target[k]
            target_doc_id = doc_id_for(design, variation)
            if target_doc_id != doc_id:
                moves.append((k, l, doc_id, target_doc_id, design, variation))

    for k, l, from_doc, to_doc, design, variation in moves:
        new_state[from_doc]['listings'] = [
            x for x in new_state[from_doc]['listings'] if lkey(x) != k
        ]
        if to_doc not in new_state:
            new_state[to_doc] = {'design': design, 'variation_type': variation, 'listings': []}
        tgt_listings = new_state[to_doc]['listings']
        if not any(lkey(x) == k for x in tgt_listings):
            tgt_listings.append(l)
        new_state[to_doc]['design'] = design
        new_state[to_doc]['variation_type'] = variation

    # Field-only corrections on docs whose listings didn't move but whose
    # own label is wrong (e.g. Bahubali_Chain: Base -> Combo, no move needed).
    for doc_id, d in existing.items():
        listings = d.get('listings') or []
        if not listings:
            continue
        targets = {final_target.get(lkey(l)) for l in listings}
        targets.discard(None)
        if len(targets) == 1:
            design, variation, _src = next(iter(targets))
            if doc_id_for(design, variation) == doc_id:
                new_state[doc_id]['design'] = design
                new_state[doc_id]['variation_type'] = variation

    docs_to_delete = [doc_id for doc_id, d in new_state.items() if not d['listings']]
    for doc_id in docs_to_delete:
        del new_state[doc_id]

    return new_state, docs_to_delete, moves


def ensure_backup(apply):
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
    for col in ('product_master',):
        out[col] = [dict(s.to_dict() or {}, _id=s.id) for s in db.collection(col).get()]
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    path = BASE / f'_firestore_backup_cleanup_{ts}.json'
    json.dump(out, open(path, 'w', encoding='utf-8'), ensure_ascii=False, indent=1, default=str)
    print(f"  backup taken: {path.name} (product_master={len(out['product_master'])})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='actually write to Firestore')
    a = ap.parse_args()

    print("=== product_master cleanup (from Jaiswal's corrected Excel) ===",
          "APPLY" if a.apply else "DRY RUN")

    final_target = load_corrections()
    print(f"\n[1] loaded corrections: {len(final_target)} listings have a confirmed target")

    existing, is_live = load_live_snapshot()
    print(f"[2] current product_master: {len(existing)} docs" + ("" if is_live else " (SNAPSHOT, not live)"))

    ensure_backup(a.apply)

    new_state, docs_to_delete, moves = build_plan(final_target, existing)

    print(f"\n[3] plan: {len(moves)} listings will move to a different doc")
    by_target = defaultdict(list)
    for k, l, from_doc, to_doc, design, variation in moves:
        by_target[to_doc].append((from_doc, l.get('sku_id') or l.get('catalog_id')))
    for to_doc, items in sorted(by_target.items()):
        print(f"    -> {to_doc}  ({len(items)} listing(s))")
        for from_doc, sku in items:
            print(f"         from {from_doc}: {sku}")

    print(f"\n[4] docs to delete (emptied by the moves above): {len(docs_to_delete)}")
    for doc_id in sorted(docs_to_delete):
        print(f"    - {doc_id}")

    field_only = [
        doc_id for doc_id, d in new_state.items()
        if doc_id in existing
        and (existing[doc_id].get('design') != d['design'] or existing[doc_id].get('variation_type') != d['variation_type'])
        and doc_id not in {m[3] for m in moves}
    ]
    print(f"\n[5] docs needing only a design/variation field correction (no listing moved): {len(field_only)}")
    for doc_id in field_only:
        old = existing[doc_id]
        new = new_state[doc_id]
        print(f"    - {doc_id}: ({old.get('design')!r}, {old.get('variation_type')!r}) -> ({new['design']!r}, {new['variation_type']!r})")

    new_docs = [doc_id for doc_id in new_state if doc_id not in existing]
    print(f"\n[6] brand-new docs to create: {len(new_docs)}")
    for doc_id in new_docs:
        d = new_state[doc_id]
        print(f"    - {doc_id}  (design={d['design']!r}, variation={d['variation_type']!r}, {len(d['listings'])} listing(s))")

    if not a.apply:
        print("\nDRY RUN — nothing written. Re-run with --apply (requires Admin SDK / GitHub Actions) after review.")
        return

    if not is_live:
        print("\nABORT: --apply requires live Firestore (Admin SDK). Cannot apply from a local snapshot.")
        sys.exit(1)

    from firestore_connector import get_db
    from datetime import datetime, timezone
    db = get_db()
    now = datetime.now(timezone.utc).isoformat()
    batch = db.batch(); n = 0
    touched_docs = set(field_only) | set(new_docs) | {m[3] for m in moves} | set(docs_to_delete)
    for doc_id in touched_docs:
        if doc_id in docs_to_delete:
            db.collection('product_master').document(doc_id).delete()
        else:
            d = new_state[doc_id]
            for l in d['listings']:
                l['updated_at'] = now
            payload = {'design': d['design'], 'variation_type': d['variation_type'], 'listings': d['listings']}
            if doc_id not in existing:
                payload['sku_id'] = folder_name(d['design'], d['variation_type'])
                payload['status'] = 'active'
                payload['created_at'] = now
            batch.set(db.collection('product_master').document(doc_id), payload, merge=True)
        n += 1
        if n % 400 == 0:
            batch.commit(); batch = db.batch()
    if n % 400:
        batch.commit()
    print(f"\nAPPLIED — {len(touched_docs)} docs touched ({len(new_docs)} created, {len(docs_to_delete)} deleted, "
          f"{len(touched_docs) - len(new_docs) - len(docs_to_delete)} updated).")


if __name__ == '__main__':
    main()
