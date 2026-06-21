"""
Seed product_master.csv → Firestore collection 'product_master'.
One document per SKU, doc ID = sku_id (with / and . replaced by _).
Run once after generating product_master.csv.
Re-run any time to update (PATCH is idempotent).
"""
import csv, json, urllib.request, urllib.error

FB_PROJECT = 'rumee-dashboard-6c4c6'
FB_API_KEY = 'AIzaSyB_5yf-YErfkaSB3o_txD3TQxVRR5KI50g'
FB_BASE = f'https://firestore.googleapis.com/v1/projects/{FB_PROJECT}/databases/(default)/documents'

def to_fs(v):
    return {"stringValue": str(v)}

def safe_id(sku_id):
    return sku_id.replace('/', '_').replace('.', '_').replace(' ', '_')

def patch_doc(collection, doc_id, fields):
    fs_fields = {k: to_fs(v) for k, v in fields.items()}
    mask = '&'.join(f'updateMask.fieldPaths={k}' for k in fields)
    url = f'{FB_BASE}/{collection}/{doc_id}?{mask}&key={FB_API_KEY}'
    body = json.dumps({'fields': fs_fields}).encode()
    req = urllib.request.Request(url, data=body, method='PATCH',
                                  headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status
    except urllib.error.HTTPError as e:
        print(f'  ERROR {e.code}: {e.read()[:200]}')
        return e.code

with open('product_master.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

print(f'Seeding {len(rows)} SKUs to Firestore…')
ok = err = 0
for row in rows:
    doc_id = safe_id(row['sku_id'])
    status = patch_doc('product_master', doc_id, row)
    if status in (200, 201):
        ok += 1
    else:
        err += 1
        print(f'  FAIL: {row["sku_id"]}')

print(f'Done. {ok} OK, {err} errors.')
