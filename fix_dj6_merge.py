#!/usr/bin/env python3
"""
One-off correction — the single DJ-6 listing that was supposed to be merged
2026-07-10 but never actually was (the workflow that would have applied it
was deleted before it ran, caught by a live-data audit afterward).

Moves meesho catalog_id 515680199 ("DJ- 6 Bahubali Six (1)") from the stray
doc DJ-_6_Bahubali into the correct DJ-6_Bahubali doc, then deletes the
now-empty stray doc. Same narrow scope as the earlier cleanup — touches
only this one listing, nothing else in product_master.

SAFE BY DEFAULT: prints the plan. Pass --apply to write (requires
FIREBASE_CREDENTIALS / GitHub Actions).
"""
import argparse, glob, sys, io
from pathlib import Path
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
BASE = Path(__file__).parent

SRC_DOC = 'DJ-_6_Bahubali'
TARGET_DOC = 'DJ-6_Bahubali'
CATALOG_ID = '515680199'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    a = ap.parse_args()

    from firestore_connector import get_db
    db = get_db()

    src = db.collection('product_master').document(SRC_DOC).get()
    target = db.collection('product_master').document(TARGET_DOC).get()
    if not src.exists:
        print(f"ABORT: {SRC_DOC} doesn't exist (already fixed, or fixed differently). Nothing to do.")
        return
    if not target.exists:
        print(f"ABORT: {TARGET_DOC} doesn't exist. Unexpected — investigate before proceeding.")
        sys.exit(1)

    src_d = src.to_dict() or {}
    target_d = target.to_dict() or {}
    src_listings = src_d.get('listings') or []
    target_listings = target_d.get('listings') or []

    moving = [l for l in src_listings if l.get('catalog_id') == CATALOG_ID]
    if not moving:
        print(f"ABORT: catalog_id {CATALOG_ID} not found in {SRC_DOC}'s listings — state has changed, investigate.")
        sys.exit(1)
    remaining_in_src = [l for l in src_listings if l.get('catalog_id') != CATALOG_ID]

    already_in_target = any(l.get('catalog_id') == CATALOG_ID for l in target_listings)

    print(f"=== fix_dj6_merge === {'APPLY' if a.apply else 'DRY RUN'}")
    print(f"  moving catalog_id {CATALOG_ID} from {SRC_DOC} -> {TARGET_DOC}")
    print(f"  already in target: {already_in_target}")
    print(f"  {SRC_DOC} will have {len(remaining_in_src)} listing(s) left -> {'DELETE doc' if not remaining_in_src else 'keep doc, write remaining listings'}")

    if not a.apply:
        print("\nDRY RUN — nothing written.")
        return

    now = datetime.now(timezone.utc).isoformat()
    if not already_in_target:
        moving[0]['updated_at'] = now
        new_target_listings = target_listings + moving
        db.collection('product_master').document(TARGET_DOC).set(
            {'listings': new_target_listings}, merge=True)
        print(f"  wrote {len(new_target_listings)} listings to {TARGET_DOC}")
    else:
        print(f"  {TARGET_DOC} already has this listing, skipping the write")

    if remaining_in_src:
        db.collection('product_master').document(SRC_DOC).set(
            {'listings': remaining_in_src}, merge=True)
        print(f"  {SRC_DOC} updated, {len(remaining_in_src)} listing(s) remain")
    else:
        db.collection('product_master').document(SRC_DOC).delete()
        print(f"  {SRC_DOC} deleted (now empty)")

    print("\nAPPLIED")


if __name__ == '__main__':
    main()
