"""
Follow-up to cleanup_legacy_product_master.py: auto-resolve needs_review
items that only exist because a legacy 'fk-*' doc had a saved fk_url with
no other data (catalog_id 'LEGACY_...').

Root cause of why these needed manual review at all (found 2026-07-02,
flagged directly by the business owner): every Flipkart buyer URL contains
the real catalog ID (FSN) as the 'pid=' query parameter — e.g.
".../p/itm80750263eb217?pid=RAKHDVJZW8GGHYTX" — RAKHDVJZW8GGHYTX IS the FSN.
The original cleanup script never extracted this, so these items were
flagged for manual assignment even though the FSN needed to auto-match them
was sitting in the URL the whole time.

This script:
  1. Reads all needs_review docs with catalog_id starting 'LEGACY_'.
  2. Extracts the pid= (FSN) from the stored fk_url.
  3. Searches existing product_master docs for that same FSN — either in
     the doc's own 'fsn' field, or in any listing's 'catalog_id'.
  4. If found: writes fk_url onto that target doc (merge, verified),
     writes a pm_overrides record pairing this needs_review item to the
     target (satisfies the delete-pairing rule in firestore.rules), then
     deletes the needs_review doc. No manual work needed for these.
  5. If not found: left alone — genuinely needs manual review (the FSN
     doesn't correspond to any doc we currently know about).

    python resolve_fk_url_needs_review.py            # dry run — report only
    python resolve_fk_url_needs_review.py --apply     # actually perform it
"""
import json
import os
import re
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

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


def extract_fsn(url):
    """Pull the pid= query param out of a Flipkart/Shopsy buyer URL — that's the FSN."""
    if not url or not url.startswith('http'):
        return None
    try:
        qs = parse_qs(urlparse(url).query)
        pid = qs.get('pid', [None])[0]
        return pid
    except Exception:
        return None


FSN_RE = re.compile(r'^[A-Z0-9]{16}$')  # Flipkart/Shopsy FSN format — guards against cross-platform catalog_id collisions


def main():
    db = get_db()

    all_docs = list(db.collection('product_master').stream())
    fsn_to_doc = {}  # {fsn: doc_id}
    for d in all_docs:
        data = d.to_dict() or {}
        if data.get('fsn') and FSN_RE.match(data['fsn']):
            fsn_to_doc.setdefault(data['fsn'], d.id)
        for lst in data.get('listings', []) or []:
            if not isinstance(lst, dict) or not lst.get('catalog_id'):
                continue
            plat = lst.get('platform', '')
            # Only index catalog_id from Flipkart/Shopsy listings, and only if
            # it matches the FSN format — prevents a Meesho catalog_id (numeric)
            # from ever being mistaken for an FSN. Found in review 2026-07-02.
            if plat in ('flipkart', 'shopsy', 'fk') and FSN_RE.match(str(lst['catalog_id'])):
                fsn_to_doc.setdefault(lst['catalog_id'], d.id)

    nr_docs = list(db.collection('needs_review').stream())
    legacy_url_items = [d for d in nr_docs if (d.to_dict() or {}).get('catalog_id', '').startswith('LEGACY_')]

    print(f"Found {len(legacy_url_items)} needs_review items from legacy fk_url-only docs "
          f"out of {len(nr_docs)} total needs_review docs.")
    print(f"Indexed {len(fsn_to_doc)} known FSNs across {len(all_docs)} product_master docs.")

    report = []
    for snap in legacy_url_items:
        data = snap.to_dict() or {}
        url = data.get('product_name', '')  # fk_url was stored here by the cleanup script
        fsn = extract_fsn(url)

        entry = {'nr_id': snap.id, 'url': url, 'extracted_fsn': fsn, 'resolved': False}

        if not fsn:
            entry['reason'] = 'could not extract pid from URL'
            report.append(entry)
            continue

        target_doc_id = fsn_to_doc.get(fsn)
        if not target_doc_id:
            entry['reason'] = 'FSN not found on any existing product_master doc — genuinely needs manual review'
            report.append(entry)
            continue

        entry['target'] = target_doc_id
        if APPLY:
            target_ref = db.collection('product_master').document(target_doc_id)
            target_ref.set({'fk_url': url}, merge=True)
            check = target_ref.get().to_dict() or {}
            if check.get('fk_url') != url:
                entry['error'] = 'fk_url verify failed — needs_review item left untouched, safe to retry'
                report.append(entry)
                continue

            override_id = f"{data.get('platform','flipkart')}_{data.get('catalog_id')}"
            db.collection('pm_overrides').document(override_id).set({
                'platform': data.get('platform', 'flipkart'), 'catalog_id': data.get('catalog_id'),
                'raw_sku': data.get('raw_sku', ''), 'target_sku_id': target_doc_id,
                'target_variation_type': '', 'assigned_at': datetime.now(timezone.utc).isoformat(),
                'assigned_by': 'auto-fsn-match',
            }, merge=True)
            db.collection('needs_review').document(snap.id).delete()
            entry['resolved'] = True
            entry['deleted'] = True

        report.append(entry)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'fsn_resolve_report_{ts}.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)

    resolved = sum(1 for r in report if r.get('resolved'))
    no_fsn = sum(1 for r in report if not r.get('extracted_fsn'))
    unmatched = sum(1 for r in report if r.get('extracted_fsn') and 'target' not in r)

    print(f"\n{'APPLIED' if APPLY else 'DRY RUN'} — report written to {report_path}")
    print(f"  total legacy fk_url items:     {len(report)}")
    print(f"  auto-resolved via FSN match:   {resolved}")
    print(f"  no pid found in URL:           {no_fsn}")
    print(f"  FSN not matched — still manual: {unmatched}")
    if not APPLY:
        print("  Run again with --apply to actually perform the resolution.")


if __name__ == '__main__':
    main()
