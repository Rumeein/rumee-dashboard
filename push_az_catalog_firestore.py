"""One-time script: push az_catalog_2026-06-30.csv to Firestore rumee_az_catalog/2026_06."""
import csv, json, os
import firebase_admin
from firebase_admin import credentials, firestore

cred_json = os.environ.get('FIREBASE_CREDENTIALS')
if not cred_json:
    raise ValueError("FIREBASE_CREDENTIALS env var not set")

cred = credentials.Certificate(json.loads(cred_json))
firebase_admin.initialize_app(cred)
db = firestore.client()

csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'az_catalog_2026-06-30.csv')
with open(csv_path, encoding='utf-8-sig') as f:
    rows = list(csv.DictReader(f))

print(f"Pushing {len(rows)} rows to Firestore rumee_az_catalog/2026_06 ...")

db.collection('rumee_az_catalog').document('2026_06').set({
    'month':     '2026-06',
    'pulled_on': '2026-06-30',
    'source':    'SP-API GET_MERCHANT_LISTINGS_ALL_DATA',
    'note':      'Pending validation — data accuracy not yet confirmed',
    'rows':      rows,
})

print(f"Done. {len(rows)} listings written to rumee_az_catalog/2026_06.")
