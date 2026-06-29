"""
One-off script: restore me_url on product_master Firestore docs by reading
PRODUCT ID from all local Meesho catalog XLSX files and building the URL:
  https://www.meesho.com/product/p/{base36(PRODUCT_ID)}

Pattern confirmed: Meesho routes product pages by base36-encoded PRODUCT ID.
The slug part of the URL is ignored — only the /p/{id} suffix matters.

Only writes where me_url is currently empty. Never overwrites an existing value.

Usage:
  python restore_me_urls.py
"""
import json, os, sys, glob
sys.path.insert(0, os.path.dirname(__file__))

# Load Firebase credentials from local file if env var not set
if not os.environ.get('FIREBASE_CREDENTIALS'):
    cred_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
    if os.path.exists(cred_path):
        with open(cred_path) as f:
            os.environ['FIREBASE_CREDENTIALS'] = f.read()

import pandas as pd
from firestore_connector import get_db

_DIGITS = '0123456789abcdefghijklmnopqrstuvwxyz'

def to_base36(n):
    if not n:
        return ''
    b36 = ''
    while n:
        b36 = _DIGITS[n % 36] + b36
        n //= 36
    return b36

import re as _re
from process import me_sku_id as _me_sku_id

def me_sku_id(raw):
    """Use process.py's authoritative me_sku_id — returns (sku_id, display_name)."""
    result = _me_sku_id(raw)
    return result[0] if isinstance(result, tuple) else result

# ── Step 1: build STYLE_ID -> me_url map from all local catalog XLSXes ────────

style_to_url  = {}   # {sku_id: me_url}
style_to_catid = {}  # {sku_id: catalog_id} — used to prefer newer (higher) catalog on conflict

# Deduplicate paths — on Windows, *.xlsx and *.XLSX both match the same files
_seen = set()
catalog_files = []
for p in glob.glob('processed/**/*.xlsx', recursive=True) + \
         glob.glob('processed/**/*.XLSX', recursive=True):
    canon = os.path.normcase(os.path.abspath(p))
    if canon not in _seen:
        _seen.add(canon)
        catalog_files.append(p)

print(f"Found {len(catalog_files)} unique XLSX files in processed/")

for path in catalog_files:
    try:
        xl  = pd.ExcelFile(path)
        df  = xl.parse(xl.sheet_names[0])
        xl.close()
        df.columns = [str(c).strip() for c in df.columns]

        style_col      = next((c for c in df.columns if 'STYLE ID'    in c.upper()), None)
        product_id_col = next((c for c in df.columns if c.upper().strip() == 'PRODUCT ID'), None)
        catalog_id_col = next((c for c in df.columns if 'CATALOG ID' in c.upper()), None)

        if not style_col or not product_id_col:
            continue  # not a catalog file

        # Skip description row if present
        if 'Row identifier' in str(df.iloc[0, 0]):
            df = df.iloc[1:].reset_index(drop=True)

        for _, row in df.iterrows():
            raw_style = str(row.get(style_col, '')).strip()
            pid_raw   = row.get(product_id_col, None)
            if not raw_style or raw_style == 'nan' or pid_raw is None:
                continue
            try:
                pid = int(float(pid_raw))
                b36 = to_base36(pid)
                if not b36:
                    continue
                sku_id = me_sku_id(raw_style)
                url = f'https://www.meesho.com/product/p/{b36}'
                # On conflict: prefer higher catalog_id (newer listing)
                cat_raw = row.get(catalog_id_col, 0) if catalog_id_col else 0
                try:
                    cat_int = int(float(cat_raw))
                except (ValueError, TypeError):
                    cat_int = 0
                if sku_id not in style_to_url or cat_int > style_to_catid.get(sku_id, 0):
                    if sku_id in style_to_url and style_to_url[sku_id] != url:
                        print(f"  Conflict {sku_id}: replacing cat {style_to_catid[sku_id]} with {cat_int} (newer)")
                    style_to_url[sku_id]   = url
                    style_to_catid[sku_id] = cat_int
            except (ValueError, TypeError):
                pass

        print(f"  {os.path.basename(path)}: contributed mappings, total unique so far: {len(style_to_url)}")
    except Exception as e:
        print(f"  Skipped {path}: {e}")

print(f"\nTotal unique SKU->URL mappings built: {len(style_to_url)}")

if not style_to_url:
    print("No mappings found — check that processed/ contains catalog XLSX files.")
    sys.exit(1)

# ── Step 2: write me_url to Firestore where currently empty ──────────────────

db = get_db()
restored        = 0
skipped_has_url = 0
skipped_no_map  = 0

for doc in db.collection('product_master').stream():
    d       = doc.to_dict()
    sku_id  = d.get('sku_id', '')
    me_url  = (d.get('me_url') or '').strip()
    platform = d.get('platform', '')

    if platform != 'me':
        continue
    if me_url:
        skipped_has_url += 1
        continue
    if sku_id not in style_to_url:
        skipped_no_map += 1
        continue

    url = style_to_url[sku_id]
    doc.reference.update({'me_url': url})
    print(f"Restored: {doc.id}  |  {url}")
    restored += 1

print(f"\nRestored: {restored} | Already had URL: {skipped_has_url} | No mapping: {skipped_no_map}")
