"""
One-off script: restore fk_url on product_master docs that have an FSN
but lost their buyer URL due to the pipeline bug (commit 191758b).

Only writes where fk_url is empty AND fsn is non-empty.
Never overwrites an existing fk_url value.

Usage:
  set FIREBASE_CREDENTIALS=<json>
  python restore_fk_urls.py
"""
import json, os, sys
sys.path.insert(0, os.path.dirname(__file__))

# Load Firebase credentials from local file if env var not set
if not os.environ.get('FIREBASE_CREDENTIALS'):
    cred_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    if os.path.exists(cred_path):
        with open(cred_path) as f:
            os.environ['FIREBASE_CREDENTIALS'] = f.read()

from firestore_connector import get_db

db = get_db()
restored = 0
skipped_has_url = 0
skipped_no_fsn  = 0

for doc in db.collection('product_master').stream():
    d      = doc.to_dict()
    fsn    = (d.get('fsn') or '').strip()
    fk_url = (d.get('fk_url') or '').strip()

    if not fsn:
        skipped_no_fsn += 1
        continue
    if fk_url:
        skipped_has_url += 1
        continue

    url = f'https://www.flipkart.com/p/itm?pid={fsn}'
    doc.reference.update({'fk_url': url})
    print(f"Restored: {doc.id}  |  {url}")
    restored += 1

print(f"\nRestored: {restored} | Already had URL: {skipped_has_url} | No FSN: {skipped_no_fsn}")
