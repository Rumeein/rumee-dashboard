"""One-off diagnostic: is Purchases -> Materials stock actually wired?
Run via GitHub Actions (correct Admin SDK project), not locally.
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

linked_lines = 0
unlinked_lines = 0
unlinked_examples = []
total_material_lines = 0

for p in purchases:
    d = p.to_dict()
    for line in d.get('details', []) or []:
        item_type = line.get('item_type')
        if item_type in ('raw_material', 'base_earring'):
            total_material_lines += 1
            mid = line.get('material_id')
            if mid:
                linked_lines += 1
            else:
                unlinked_lines += 1
                if len(unlinked_examples) < 10:
                    unlinked_examples.append({
                        'purchase_id': d.get('purchase_id'),
                        'date': d.get('date'),
                        'item_type': item_type,
                        'item_name': line.get('item_name'),
                    })

print(f"Purchase detail lines with item_type raw_material/base_earring: {total_material_lines}")
print(f"  -> linked to a material_id: {linked_lines}")
print(f"  -> NOT linked (material_id blank): {unlinked_lines}")
print()
print("Examples of unlinked lines (up to 10):")
for e in unlinked_examples:
    print(f"  {e}")
print()

purchase_ledger_entries = [l.to_dict() for l in ledger if l.to_dict().get('source_type') == 'purchase']
print(f"stock_ledger entries with source_type='purchase': {len(purchase_ledger_entries)}")
print()

print("Materials with non-zero current_stock or current_avg_cost:")
for mid, m in materials.items():
    stock = m.get('current_stock', 0)
    avg = m.get('current_avg_cost', 0)
    if stock or avg:
        print(f"  {m.get('name')}: stock={stock}, avg_cost={avg}")
