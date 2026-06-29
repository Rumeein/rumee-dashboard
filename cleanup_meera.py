"""
One-off script: delete all product_master Firestore docs from the old
"Meera Craft Store" seller account. Run once, then delete this file.

Usage:
  set FIREBASE_CREDENTIALS=<json>
  python cleanup_meera.py
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
deleted = 0
for doc in db.collection('product_master').stream():
    d = doc.to_dict()
    combined = (d.get('sku_name', '') + ' ' + d.get('sku_id', '')).lower()
    if 'meera craft store' in combined:
        doc.reference.delete()
        print(f"Deleted: {doc.id}  |  {d.get('sku_name', '')}")
        deleted += 1

print(f"\nTotal deleted: {deleted}")
