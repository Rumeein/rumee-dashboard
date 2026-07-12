"""One-off diagnostic #2: did the retroactive stock-posting fix actually
work on a real save? Run via GitHub Actions (correct Admin SDK project).
Deleted after use.
"""
import os
import json
import firebase_admin
from firebase_admin import credentials, firestore

cred_json = os.environ['FIREBASE_CREDENTIALS']
cred = credentials.Certificate(json.loads(cred_json))
firebase_admin.initialize_app(cred)
db = firestore.client()

purchases = list(db.collection('rumee_purchases').stream())
materials = {d.id: d.to_dict() for d in db.collection('rumee_materials').stream()}
ledger = list(db.collection('rumee_stock_ledger').stream())

print(f"Total purchases: {len(purchases)}")
print(f"Total materials: {len(materials)}")
print(f"Total stock_ledger entries: {len(ledger)}")
print()

# Sort purchases by entered_at descending, show the 5 most recent in full
purchases_sorted = sorted(purchases, key=lambda p: (p.to_dict().get('entered_at') or ''), reverse=True)
print("=== 5 most recently entered purchases (full detail) ===")
for p in purchases_sorted[:5]:
    d = p.to_dict()
    print(f"\npurchase_id={d.get('purchase_id')}  date={d.get('date')}  vendor={d.get('vendor')}  entered_at={d.get('entered_at')}  amount_paid={d.get('amount_paid')}")
    for line in d.get('details', []) or []:
        print(f"  item_type={line.get('item_type')}  item_name={line.get('item_name')}  material_id={line.get('material_id')}  stock_posted={line.get('stock_posted')}  qty_received={line.get('qty_received')}  rate={line.get('rate')}")

print()
print("=== Search for 'Dooney'/'Duney' in vendor or item names ===")
found = False
for p in purchases:
    d = p.to_dict()
    vendor = str(d.get('vendor') or '')
    if 'oon' in vendor.lower() or 'uney' in vendor.lower():
        found = True
        print(f"  MATCH: purchase_id={d.get('purchase_id')} vendor={vendor} date={d.get('date')} amount_paid={d.get('amount_paid')}")
        for line in d.get('details', []) or []:
            print(f"    item_type={line.get('item_type')}  item_name={line.get('item_name')}  material_id={line.get('material_id')}  stock_posted={line.get('stock_posted')}")
if not found:
    print("  No purchase found with vendor matching 'Dooney'/'Duney'.")

print()
print("=== stock_ledger entries with source_type='purchase' ===")
purchase_ledger = [l.to_dict() for l in ledger if l.to_dict().get('source_type') == 'purchase']
print(f"Count: {len(purchase_ledger)}")
for e in purchase_ledger:
    print(f"  {e}")

print()
print("=== Materials with non-zero current_stock or current_avg_cost ===")
for mid, m in materials.items():
    stock = m.get('current_stock', 0)
    avg = m.get('current_avg_cost', 0)
    if stock or avg:
        print(f"  {m.get('name')}: stock={stock}, avg_cost={avg}, entered_by={m.get('entered_by')}")
