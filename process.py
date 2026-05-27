"""
Rumee Dashboard Data Pipeline
Processes raw export files from Meesho and Flipkart seller panels,
updates rumee_db_v1.csv, and bumps the date in index.html.

Usage:
    1. Drop raw export files into the new_data/ folder
    2. Run: python process.py
    3. Files are archived to processed/YYYY-MM-DD/

File type auto-detection:
    - Meesho Orders CSV:     contains "Reason for Credit Entry" column
    - Meesho Returns CSV:    contains "Type of Return" or "Meesho Supplier Panel" header
    - Meesho Payments XLSX:  contains "Order Related Details - Sub Order No"
    - Meesho Ads XLSX:       contains "Ads Cost - Ad Cost"
    - FK Payments XLSX:      contains "Order Details - Seller SKU" + "Bank Settlement"
    - FK Ads XLSX:           contains "Wallet Redeem"
    - FK Views CSV:          contains "Product Views" + "SKU Id" + "Impression Date"
    - FK Keywords CSV:       contains "attributed_keyword_views"
    - Catalog XLSX:          contains "SYSTEM STOCK" or "STYLE ID"
"""

import os, sys, shutil, re, glob, csv, argparse, json, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date, datetime
from pathlib import Path
import pandas as pd

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
NEW_DATA   = BASE_DIR / "new_data"
PROCESSED  = BASE_DIR / "processed"
DB_PATH          = BASE_DIR / "rumee_db_v1.csv"        # legacy — kept during transition
DB_SUMMARY_PATH  = BASE_DIR / "rumee_db_summary.csv"   # dashboard overview (replaces v1)
DB_DAILY_PATH    = BASE_DIR / "rumee_db_daily.csv"     # rolling 6-month daily rows
DB_KEYWORDS_PATH = BASE_DIR / "rumee_db_keywords.csv"  # full keyword history
DB_ALLTIME_PATH  = BASE_DIR / "rumee_db_alltime.csv"   # all-time daily (on-demand only)
HTML_PATH        = BASE_DIR / "index.html"
TODAY            = date.today().isoformat()

# ─── SKU Mappings ─────────────────────────────────────────────────────────────
# Meesho: raw SKU string -> dashboard sku_id, display_name
ME_SKU_MAP = {
    "DJ-5 Bahubali Five":    ("dj5-me",       "DJ-5 Bahubali Five"),
    "DJ-5":                  ("dj5-me",       "DJ-5 Bahubali Five"),
    "DJ- 6 Bahubali Six":    ("dj6-me",       "DJ-6 Bahubali Six"),
    "DJ-6 Bahubali Six":     ("dj6-me",       "DJ-6 Bahubali Six"),
    "DJ- 6 Bahubali":        ("dj6-me",       "DJ-6 Bahubali Six"),
    "DJ-1 Bahubali S":       ("dj1-me",       "DJ-1 Bahubali S"),
    "Bahubali DJ1 Small":    ("dj1-me",       "DJ-1 Bahubali S"),
    "DJ-1 S Bahubali (1)":   ("dj1-me",       "DJ-1 Bahubali S"),
    "DJ-11 BAHUBALI":        ("dj11-me",      "DJ-11 BAHUBALI"),
    "DJ-7 Bahubali":         ("dj7-me",       "DJ-7 Bahubali"),
    "DJ-7 Bahubali (2)":     ("dj7-me",       "DJ-7 Bahubali"),
    "DJ 14 Bahubali":        ("dj14-me",      "DJ 14 Bahubali"),
    "Coin Pearl Choker":     ("coin-choker",  "Coin Pearl Choker"),
    "OG DJ-7":               ("ogdj7-me",     "OG DJ-7"),
    "OG DJ7":                ("ogdj7-me",     "OG DJ-7"),
    "OG DJ5 Five":           ("ogdj5-me",     "OG DJ5 Five"),
    "OG DJ-6":               ("ogdj6-me",     "OG DJ-6"),
    "OG DJ-11":              ("ogdj11-me",    "OG DJ-11"),
    "OG DJ 14":              ("ogdj14-me",    "OG DJ 14"),
    "OG DJ-13":              ("ogdj13-me",    "OG DJ-13"),
    "DJ-3 Bahubali Three":   ("dj3-me",       "DJ-3 Bahubali Three"),
    "DJ-3":                  ("dj3-me",       "DJ-3 Bahubali Three"),
    "Original NJ2":          ("nj2-me",       "Original NJ2"),
    "DJ9":                   ("dj9-me",       "DJ-9"),
    "DJ-13 BAHUBALI":        ("dj13-me",      "DJ-13 BAHUBALI"),
    "New Combo 1":           ("combo1-me",    "New Combo 1"),
    "New Combo 3":           ("combo3-me",    "New Combo 3"),
    "Bahubali Chain COMBO 1":("bcombo1-me",   "Bahubali Chain COMBO 1"),
    "Bahubali DJ7":          ("dj7-me",       "DJ-7 Bahubali"),
    "DJ Bahu":               ("djbahu-me",    "DJ Bahu"),
    "SC8":                   ("sc8-me",       "SC8"),
    "DJ-Bahubali":           ("djbahu-me",    "DJ Bahu"),
}

# Flipkart: Seller SKU -> dashboard sku_id, display_name
FK_SKU_MAP = {
    "DJ-5 Bahubali":        ("dj5b",   "DJ-5 Bahubali"),
    "Bahubali DJ7":         ("dj7b",   "Bahubali DJ7"),
    "DJ7":                  ("dj7b",   "Bahubali DJ7"),
    "DJ-6 Bahubali":        ("dj6b",   "DJ-6 Bahubali"),
    "DJ-11 BAHUBALI":       ("dj11b",  "DJ-11 Bahubali"),
    "DJ 14 Bahubali":       ("dj14b",  "DJ-14 Bahubali"),
    "Bahubali DJ3":         ("dj3b",   "Bahubali DJ3"),
    "DJ-3 Bahubali (1)":   ("dj3b",   "Bahubali DJ3"),
    "Bahubali DJ1 Small":   ("dj1b",   "Bahubali DJ1 Small"),
    "DJ1 Small":            ("dj1b",   "Bahubali DJ1 Small"),
    "OG DJ6":               ("ogdj6",  "OG DJ-6"),
    "OG DJ5":               ("ogdj5",  "OG DJ-5"),
    "OG DJ 14":             ("ogdj14", "OG DJ-14"),
    "DJ-5 Bahu (2)":        ("dj5b2",  "DJ-5 Bahu (2)"),
    "DJ-4 Bahubali":        ("dj4b",   "DJ-4 Bahubali"),
    "NJO-2":                ("njo2",   "NJO-2 Silver Bahubali"),
    "NJ2-1":                ("nj2-1",  "NJ2-1"),
    "NJ Small":             ("nj-sm",  "NJ Small"),
    "NJ Mini":              ("nj-mini","NJ Mini"),
    "Coin Pearl Choker":    ("coin-fk","Coin Pearl Choker"),
    "BANGLE-5 FIVE":        ("bangle", "BANGLE-5 FIVE"),
    "BANGLE-4":             ("bangle4","BANGLE-4"),
    "GB1":                  ("gb1",    "GB1"),
    "OG DJ-12 PINK":        ("dj12p",  "DJ-12 Pink Kashmiri"),
    "OG DJ-11":             ("ogdj11", "OG DJ-11"),
    "OG DJ-13":             ("ogdj13", "OG DJ-13"),
    "DJ8":                  ("dj8",    "DJ-8"),
    "DJ9":                  ("dj9-fk", "DJ-9"),
}

MONTH_LABELS = {
    "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun",
    "07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec",
}

def flatten_multiindex_columns(df):
    """Flatten a pandas MultiIndex column to single-level strings.
    e.g. ('Order Details', 'Seller SKU') -> 'Order Details - Seller SKU'
    Useful after pd.read_excel(..., header=[0,1]) when you want named access.
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            ' - '.join(
                str(c) for c in col
                if str(c) not in ('', 'nan', 'Unnamed: 0_level_0')
            ).strip(' -')
            for col in df.columns
        ]
    return df

def month_key(date_str):
    """Convert date string to YYYY-MM month key."""
    if not date_str or pd.isna(date_str):
        return None
    s = str(date_str)[:10]
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m")
    except Exception:
        return None

def month_label(mk):
    """Convert YYYY-MM to short label."""
    if mk and len(mk) >= 7:
        return MONTH_LABELS.get(mk[5:7], mk[5:7])
    return ""

def me_sku_id(raw_sku):
    """Map raw Meesho SKU to (sku_id, display_name)."""
    raw = str(raw_sku).strip()
    if raw in ME_SKU_MAP:
        return ME_SKU_MAP[raw]
    slug = re.sub(r'[^a-z0-9]', '-', raw.lower()).strip('-')
    return (f"me-{slug}", raw)

def fk_sku_id(raw_sku):
    """Map raw FK SKU to (sku_id, display_name)."""
    raw = str(raw_sku).strip()
    if raw in FK_SKU_MAP:
        return FK_SKU_MAP[raw]
    slug = re.sub(r'[^a-z0-9]', '-', raw.lower()).strip('-')
    return (f"fk-{slug}", raw)

# ─── DB Load/Save ─────────────────────────────────────────────────────────────

def load_db(path):
    """Load multi-table CSV into dict of {table_name: [row_dict, ...]}."""
    db = {}
    if not path.exists():
        return db
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = None
        for row in reader:
            if not row or not row[0]:
                continue
            if row[0] == '__table__':
                headers = row[1:]
                continue
            if headers is None:
                continue
            table = row[0]
            rec = {}
            for i, h in enumerate(headers):
                v = row[i+1] if i+1 < len(row) else ''
                try:
                    rec[h] = float(v) if v not in ('', None) and v.replace('.','',1).replace('-','',1).isdigit() else v
                except Exception:
                    rec[h] = v
            db.setdefault(table, []).append(rec)
    return db

def save_db(db, path):
    """Write multi-table CSV from dict."""
    table_schemas = {
        'config':           ['key', 'value'],
        'fk_monthly':       ['month', 'label', 'gmv', 'settlement', 'orders', 'returns', 'ad_spend'],
        'me_monthly':       ['month', 'label', 'gmv', 'settlement', 'orders', 'returns', 'ad_spend'],
        'fk_skus':          ['sku_id', 'name', 'type', 'mrp', 'selling', 'settlement', 'stock',
                             'ctr', 'ad_revenue', 'conversions', 'ad_views'],
        'me_skus':          ['sku_id', 'name', 'type', 'total_orders', 'delivered', 'rto',
                             'cust_returns', 'return_rate', 'cust_ret_rate', 'rto_rate',
                             'gmv', 'avg_price', 'incomplete', 'wrong_product', 'quality'],
        'me_return_reasons':['reason', 'count', 'pct'],
        'fk_pairs':         ['base', 'og_name', 'og_mrp', 'og_selling', 'og_settlement',
                             'bahu_name', 'bahu_mrp', 'bahu_selling', 'bahu_settlement',
                             'status', 'verdict'],
        'az_monthly':       ['month', 'label', 'gmv', 'orders', 'ad_spend'],
        'fk_keywords':      ['keyword', 'views', 'clicks', 'orders', 'revenue',
                             'ctr', 'conversion_rate'],
        'me_claims':        ['order_id', 'suborder_id', 'ticket_id', 'status', 'issue_type',
                             'created_date', 'last_update', 'reopen_validity',
                             'amount_recovered', 'transaction_id'],
        'fk_claims':        ['claim_id', 'incident_id', 'order_id', 'order_item_id', 'source',
                             'created_at', 'updated_at', 'status', 'approved_amount',
                             'not_approved_reason', 'auto_claim_reason'],
    }
    table_order = list(table_schemas.keys())
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for tname in table_order:
            if tname not in db:
                continue
            cols = table_schemas[tname]
            w.writerow(['__table__'] + cols)
            for rec in db[tname]:
                row = [tname] + [rec.get(c, '') for c in cols]
                w.writerow(row)
    print(f"  Saved DB: {path}")

# Schemas for the 3 new split files
_DAILY_SCHEMAS = {
    'fk_daily': ['date', 'sku_id', 'sku_name', 'views', 'clicks', 'sales',
                 'revenue', 'ctr', 'conversion_rate'],
    'me_daily': ['date', 'sku_id', 'sku_name', 'orders_placed', 'delivered',
                 'rto', 'cancelled', 'gmv', 'returns_received',
                 'top_return_reason', 'states'],
}
_KEYWORDS_SCHEMA = ['month', 'sku_id', 'sku_name', 'keyword',
                    'total_views', 'impression_pct', 'attributed_views']


def save_daily_csv(fk_rows, me_rows, path):
    """Write rumee_db_daily.csv with fk_daily and me_daily tables.
    Returns (total_rows, min_date_str, max_date_str)."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for tname, cols in _DAILY_SCHEMAS.items():
            rows = fk_rows if tname == 'fk_daily' else me_rows
            w.writerow(['__table__'] + cols)
            for rec in rows:
                w.writerow([tname] + [rec.get(c, '') for c in cols])
    all_dates = [r['date'] for r in fk_rows + me_rows if r.get('date')]
    d_min = min(all_dates) if all_dates else ''
    d_max = max(all_dates) if all_dates else ''
    total = len(fk_rows) + len(me_rows)
    rng   = f"{d_min} to {d_max}" if all_dates else 'no data'
    print(f"  Saved rumee_db_daily.csv:    {total} rows ({rng})")
    return total, d_min, d_max


def save_keywords_csv(kw_rows, path):
    """Write rumee_db_keywords.csv with fk_keywords table."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['__table__'] + _KEYWORDS_SCHEMA)
        for rec in kw_rows:
            w.writerow(['fk_keywords'] + [rec.get(c, '') for c in _KEYWORDS_SCHEMA])
    print(f"  Saved rumee_db_keywords.csv: {len(kw_rows)} keyword-month-SKU rows")


def get_config(db, key, default='1970-01-01'):
    for r in db.get('config', []):
        if r.get('key') == key:
            return str(r.get('value', default))
    return default

def set_config(db, key, value):
    rows = db.setdefault('config', [])
    for r in rows:
        if r.get('key') == key:
            r['value'] = value
            return
    rows.append({'key': key, 'value': value})

# ─── File Type Detection ───────────────────────────────────────────────────────

def sniff_csv_header(path):
    """Read first 10 lines of a CSV as a combined string for detection."""
    try:
        with open(path, newline='', encoding='utf-8', errors='replace') as f:
            return '\n'.join([f.readline() for _ in range(10)]).lower()
    except Exception:
        return ''

def sniff_xlsx_header(path):
    """Read first sheet, first row of XLSX for detection."""
    try:
        df = pd.read_excel(path, nrows=3, header=None)
        return ' '.join([str(v).lower() for v in df.values.flatten() if pd.notna(v)])
    except Exception:
        return ''

def sniff_xlsx_sheets(path):
    """Return list of sheet names from an XLSX without reading data."""
    try:
        return pd.ExcelFile(path).sheet_names
    except Exception:
        return []

def detect_file_type(path):
    """Return one of: ME_ORDERS, ME_RETURNS, ME_PAYMENTS, ME_ADS,
                      FK_PAYMENTS, FK_ADS, FK_ADS_CAMPAIGN, FK_VIEWS,
                      FK_KEYWORDS, FK_LISTINGS, CATALOG, UNKNOWN"""
    ext = path.suffix.lower()
    if ext == '.csv':
        hdr = sniff_csv_header(path)
        if 'reason for credit entry' in hdr:
            return 'ME_ORDERS'
        if 'type of return' in hdr or 'meesho supplier panel' in hdr:
            return 'ME_RETURNS'
        if 'attributed_keyword_views' in hdr:
            return 'FK_KEYWORDS'
        if 'product views' in hdr and 'sku id' in hdr and 'impression date' in hdr:
            return 'FK_VIEWS'
        # Meesho Seller Support tickets / claims export
        if 'ticket id' in hdr and 'order number' in hdr and 'ticket status' in hdr:
            return 'ME_CLAIMS'
        return 'UNKNOWN'
    elif ext in ('.xlsx', '.xls'):
        hdr = sniff_xlsx_header(path)
        # Meesho payment (has 'Order Related Details' multi-header)
        if 'order related details' in hdr and 'sub order no' in hdr:
            return 'ME_PAYMENTS'
        # Meesho standalone ads cost sheet
        if 'ads cost' in hdr and ('ad cost' in hdr or 'deduction' in hdr) and 'flipkart' not in hdr:
            return 'ME_ADS'
        # FK campaign performance report (Consolidate ad report)
        if 'campaign budget' in hdr and 'campaign_start_date' in hdr:
            return 'FK_ADS_CAMPAIGN'
        if 'wallet redeem' in hdr or ('flipkart' in str(path).lower() and 'ads' in str(path).lower() and 'wallet' in hdr):
            return 'FK_ADS'
        # FK Listing file: has 'listing id' and 'listing status' — check BEFORE FK_PAYMENTS
        if 'listing id' in hdr and 'listing status' in hdr:
            return 'FK_LISTINGS'
        if 'seller sku' in hdr and ('settlement' in hdr or 'bank settlement' in hdr or 'sale amount' in hdr):
            return 'FK_PAYMENTS'
        if 'system stock' in hdr or ('style id' in hdr and 'catalog' in hdr):
            return 'CATALOG'
        # Sheet-name based detection for multi-sheet FK payment files
        sheets = sniff_xlsx_sheets(path)
        sheets_lower = [s.lower() for s in sheets]
        if 'orders' in sheets_lower and 'gst_details' in sheets_lower:
            return 'FK_PAYMENTS'
        if 'order payments' in sheets_lower and 'ads cost' in sheets_lower:
            return 'ME_PAYMENTS'
        if 'overall performance report' in sheets_lower or 'campaign summary' in sheets_lower:
            return 'FK_ADS_CAMPAIGN'
        # Also check sheet names for listing file
        if any('listing' in s.lower() for s in sheets):
            if 'listing' in path.stem.lower() or 'listing file' in path.stem.lower():
                return 'FK_LISTINGS'
        # FK Claims XLSX: has 'Seller Claims' or 'Auto-Approved Claims' sheets
        if any('claim' in s.lower() for s in sheets):
            return 'FK_CLAIMS'
        # Fallback: try by filename
        name = path.stem.lower()
        if 'listing' in name and 'flipkart' not in name and 'orders' not in name:
            return 'FK_LISTINGS'
        if 'flipkart_ads' in name or ('ads_data' in name and 'flipkart' in name):
            return 'FK_ADS'
        if 'flipkart_payment' in name or 'payment_data' in name:
            return 'FK_PAYMENTS'
        if 'ads_cost' in name or ('ads' in name and 'meesho' in name):
            return 'ME_ADS'
        if 'order_payment' in name or ('payment' in name and 'meesho' in name):
            return 'ME_PAYMENTS'
        if 'catelog' in name or 'catalog' in name or 'inventory' in name:
            return 'CATALOG'
        return 'UNKNOWN'
    return 'UNKNOWN'

# ─── Meesho Orders ────────────────────────────────────────────────────────────

def process_meesho_orders(path, last_date_str):
    """
    Returns:
        monthly: {month: {gmv, orders, returns, ...}}
        skus:    {sku_id: {name, delivered, rto, gmv, avg_price, ...}}
        new_last_date: str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(path, dtype={'Order Date': str})

    # Parse dates
    df['_dt'] = pd.to_datetime(df['Order Date'], errors='coerce').dt.date
    before = len(df)
    df = df[df['_dt'].notna()]
    df_new = df[df['_dt'] > last_date]
    df_skip = df[df['_dt'] <= last_date]
    new_last = df['_dt'].max() if len(df) else last_date

    print(f"  ME Orders: {len(df_new)} new rows ({df_new['_dt'].min()} to {df_new['_dt'].max() if len(df_new) else 'N/A'}), "
          f"skipping {len(df_skip)} already-processed rows")
    if len(df_new) == 0:
        return {}, {}, str(new_last)

    status_col   = 'Reason for Credit Entry'
    price_col    = 'Supplier Discounted Price (Incl GST and Commision)'
    listed_col   = 'Supplier Listed Price (Incl. GST + Commission)'
    sku_col      = 'SKU'

    monthly = {}
    skus    = {}

    for _, row in df_new.iterrows():
        status = str(row.get(status_col, '')).strip()
        mk     = month_key(str(row['_dt']))
        if not mk:
            continue
        price  = float(row.get(price_col, 0) or 0)
        raw_sku = str(row.get(sku_col, '')).strip()
        sid, sname = me_sku_id(raw_sku)

        m = monthly.setdefault(mk, {'gmv':0,'orders':0,'returns':0})
        s = skus.setdefault(sid, {
            'name':sname,'type':'','delivered':0,'rto':0,'cancelled':0,
            'gmv':0,'prices':[]
        })

        if status == 'DELIVERED':
            m['gmv']    += price
            m['orders'] += 1
            s['delivered'] += 1
            s['gmv']    += price
            s['prices'].append(price)
        elif status == 'RTO_COMPLETE':
            m['returns'] += 1
            s['rto'] += 1
        elif status in ('CANCELLED', 'LOST'):
            s['cancelled'] += 1
        # SHIPPED / READY_TO_SHIP / RTO_OFD / RTO_LOCKED / RTO_INITIATED / HOLD = in transit, skip

    # Compute SKU averages
    for sid, s in skus.items():
        s['avg_price'] = round(sum(s['prices']) / len(s['prices']), 2) if s['prices'] else 0
        del s['prices']
        total = s['delivered'] + s['rto']
        s['return_rate']  = round((s['rto']) / total * 100, 2) if total else 0
        s['rto_rate']     = s['return_rate']
        s['cust_ret_rate']= 0   # filled from returns file
        s['cust_returns'] = 0
        s['incomplete']   = 0
        s['wrong_product']= 0
        s['quality']      = 0
        s['total_orders'] = s['delivered'] + s['rto']

    return monthly, skus, str(new_last)

# ─── Meesho Returns ───────────────────────────────────────────────────────────

def process_meesho_returns(path, last_date_str):
    """
    Returns:
        sku_returns: {sku_id: {cust_returns, incomplete, wrong_product, quality}}
        reasons:     {reason_str: count}
        new_last_date: str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()

    # File has 7 header rows; column headers are on row 8 (index 7)
    with open(path, newline='', encoding='utf-8', errors='replace') as f:
        content = f.read()
    # Find actual header line
    lines = content.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if '"S No"' in line or 'S No' in line and 'Product Name' in line:
            header_idx = i
            break
    if header_idx is None:
        # Try auto-detect
        df = pd.read_csv(path, skiprows=7)
    else:
        df = pd.read_csv(path, skiprows=header_idx)

    # Strip quotes from column names
    df.columns = [c.strip('"').strip() for c in df.columns]

    # Date column — prefer 'Return Created Date'; fall back to 'Dispatch Date'
    date_col = (next((c for c in df.columns if 'Return Created Date' in c), None)
                or next((c for c in df.columns if 'Dispatch Date' in c), None))
    sku_col  = next((c for c in df.columns if c == 'SKU'), 'SKU')
    type_col = next((c for c in df.columns if 'Type of Return' in c), None)
    reason_col = next((c for c in df.columns if 'Detailed Return Reason' in c), None)
    sub_reason_col = next((c for c in df.columns if 'Return Reason' in c and 'Detailed' not in c), None)

    df['_dt'] = pd.to_datetime(df.get(date_col, pd.Series(dtype=str)), errors='coerce').dt.date
    before = len(df)
    df = df[df['_dt'].notna()]
    df_new = df[df['_dt'] > last_date]
    df_skip = df[df['_dt'] <= last_date]
    new_last = df['_dt'].max() if len(df) else last_date

    print(f"  ME Returns: {len(df_new)} new rows ({df_new['_dt'].min() if len(df_new) else 'N/A'} to "
          f"{df_new['_dt'].max() if len(df_new) else 'N/A'}), skipping {len(df_skip)}")
    if len(df_new) == 0:
        return {}, {}, str(new_last)

    sku_returns = {}
    reasons     = {}

    for _, row in df_new.iterrows():
        raw_sku = str(row.get(sku_col, '')).strip().strip('"')
        ret_type = str(row.get(type_col, '') if type_col else '').strip('"').strip()
        reason_detail = str(row.get(reason_col, '') if reason_col else '').strip('"').strip()
        reason_sub    = str(row.get(sub_reason_col, '') if sub_reason_col else '').strip('"').strip()

        sid, _ = me_sku_id(raw_sku)
        s = sku_returns.setdefault(sid, {
            'cust_returns':0,'incomplete':0,'wrong_product':0,'quality':0
        })

        if 'customer return' in ret_type.lower():
            s['cust_returns'] += 1
            # Categorise reason
            reason_low = (reason_detail + ' ' + reason_sub).lower()
            if any(k in reason_low for k in ['incomplete','missing piece','part']):
                s['incomplete'] += 1
            elif any(k in reason_low for k in ['wrong','different','not ordered','different color','different product']):
                s['wrong_product'] += 1
            elif any(k in reason_low for k in ['quality','defective','broken','torn','stain','damage']):
                s['quality'] += 1
            # Track reason
            r_key = reason_detail if reason_detail and reason_detail != 'NA' else reason_sub
            if r_key and r_key != 'NA' and r_key != 'nan':
                reasons[r_key] = reasons.get(r_key, 0) + 1

    return sku_returns, reasons, str(new_last)

# ─── Meesho Payments ──────────────────────────────────────────────────────────

def process_meesho_payments(path, last_date_str, ads_last_date_str=None):
    """
    Handles single-sheet (legacy) and multi-sheet (v2) Meesho payment files.

    Multi-sheet format:
        Sheet 'Order Payments'          -> settlement data
        Sheet 'Ads Cost'                -> ads spend (same as standalone ME_ADS)
        Sheet 'Compensation and Recovery' -> logged, not stored yet

    Positional columns (Order Payments sheet):
        col 1  = Order Date
        col 13 = Final Settlement Amount

    Positional columns (Ads Cost sheet):
        col 1 = Deduction Date
        col 7 = Total Ads Cost (negative value)

    Args:
        path:               Path to payment XLSX
        last_date_str:      Last processed date for settlement (me_payments_last_date)
        ads_last_date_str:  Last processed date for ads (me_ads_last_date);
                            defaults to last_date_str if None

    Returns:
        monthly_sett:   {month: settlement_float}
        monthly_ads:    {month: ad_spend_float}  -- empty if no Ads Cost sheet
        pay_new_last:   str  -- new last date for settlement
        ads_new_last:   str  -- new last date for ads (unchanged if no ads sheet)
    """
    if ads_last_date_str is None:
        ads_last_date_str = last_date_str

    last_date     = datetime.strptime(last_date_str,     '%Y-%m-%d').date()
    ads_last_date = datetime.strptime(ads_last_date_str, '%Y-%m-%d').date()

    xl = pd.ExcelFile(path)
    sheet_names = xl.sheet_names

    # ── Find the order-payments sheet ────────────────────────────────────────
    orders_sheet = next(
        (s for s in sheet_names if 'order' in s.lower()),
        sheet_names[0]   # fallback: first sheet
    )

    # ── Process settlement data ───────────────────────────────────────────────
    df = xl.parse(orders_sheet, header=[0, 1])
    dates = pd.to_datetime(df.iloc[:, 1],  errors='coerce').dt.date   # col 1 = Order Date
    setts = pd.to_numeric(df.iloc[:, 13], errors='coerce').fillna(0)  # col 13 = Settlement

    valid = dates.notna()
    df2   = pd.DataFrame({'_dt': dates[valid], 'sett': setts[valid]})
    df_new = df2[df2['_dt'] > last_date]
    pay_new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  ME Payments (orders): {len(df_new)} new rows, "
          f"skipping {len(df2) - len(df_new)}")

    monthly_sett = {}
    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if mk:
            monthly_sett[mk] = monthly_sett.get(mk, 0) + float(row['sett'])
    monthly_sett = {k: round(v, 2) for k, v in monthly_sett.items()}

    # ── Process ads sheet (if present) ───────────────────────────────────────
    monthly_ads  = {}
    ads_new_last = ads_last_date

    ads_sheet = next(
        (s for s in sheet_names if 'ads' in s.lower() and 'order' not in s.lower()),
        None
    )

    if ads_sheet:
        try:
            df_ads = xl.parse(ads_sheet, header=[0, 1])
            # Keep as datetime64 for consistent comparison with pd.Timestamp
            ad_dates = pd.to_datetime(df_ads.iloc[:, 1], errors='coerce')  # col 1 = Date
            ad_costs = pd.to_numeric(df_ads.iloc[:, 7], errors='coerce').fillna(0)  # col 7 = Cost

            valid_a    = ad_dates.notna()
            df_ads2    = pd.DataFrame({'_dt': ad_dates[valid_a], 'cost': ad_costs[valid_a]})
            ads_cutoff = pd.Timestamp(ads_last_date)
            df_ads_new = df_ads2[df_ads2['_dt'] > ads_cutoff]
            ads_new_last = df_ads2['_dt'].dt.date.max() if len(df_ads2) else ads_last_date

            print(f"  ME Payments (ads):    {len(df_ads_new)} new rows, "
                  f"skipping {len(df_ads2) - len(df_ads_new)}")

            for _, row in df_ads_new.iterrows():
                mk = month_key(str(row['_dt'])[:10])
                if mk:
                    monthly_ads[mk] = monthly_ads.get(mk, 0) + abs(float(row['cost']))
            monthly_ads = {k: round(v, 2) for k, v in monthly_ads.items()}

        except Exception as e:
            print(f"  ME Payments (ads sheet): error - {e}")

    # Log compensation sheet if present (not stored in DB yet)
    comp_sheet = next(
        (s for s in sheet_names if 'comp' in s.lower() or 'recov' in s.lower()),
        None
    )
    if comp_sheet:
        try:
            df_comp = xl.parse(comp_sheet)
            print(f"  ME Payments (compensation): {len(df_comp)} rows (logged only, not stored)")
        except Exception:
            pass

    return monthly_sett, monthly_ads, str(pay_new_last), str(ads_new_last)

# ─── Meesho Ads ───────────────────────────────────────────────────────────────

def process_meesho_ads(path, last_date_str):
    """Returns monthly: {month: ad_spend} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0], header=[0, 1])

    # Positional: col 1 = Deduction Date, col 7 = Total Ads Cost (negative)
    date_col  = 1
    cost_col  = 7

    dates = pd.to_datetime(df.iloc[:, date_col], errors='coerce').dt.date
    costs = pd.to_numeric(df.iloc[:, cost_col], errors='coerce').fillna(0)

    valid = dates.notna()
    df2 = pd.DataFrame({'_dt': dates[valid], 'cost': costs[valid]})
    df_new = df2[df2['_dt'] > last_date]
    new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  ME Ads: {len(df_new)} new rows, skipping {len(df2)-len(df_new)}")
    if len(df_new) == 0:
        return {}, str(new_last)

    monthly = {}
    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if mk:
            # Total Ads Cost is negative (deduction), abs = spend
            monthly[mk] = monthly.get(mk, 0) + abs(float(row['cost']))

    return {k: round(v, 2) for k, v in monthly.items()}, str(new_last)

# ─── Flipkart Payments ────────────────────────────────────────────────────────

def process_fk_payments(path, last_date_str, ads_last_date_str=None):
    """
    Handles single-sheet (legacy) and multi-sheet (v2) FK payment files.

    Multi-sheet format:
        Sheet 'Orders'      -> order-level data (gmv, settlement, SKU, return type)
        Sheet 'Ads'         -> ads spend (same as standalone FK_ADS)
        Sheet 'GST_Details' -> logged only, not stored yet

    Positional columns (Orders sheet with 2-row header):
        col 3  = Bank Settlement Value
        col 9  = Sale Amount
        col 55 = Order Date
        col 58 = Seller SKU
        col 62 = Return Type

    Args:
        path:               Path to payment XLSX
        last_date_str:      Last processed date for orders (fk_payments_last_date)
        ads_last_date_str:  Last processed date for ads (fk_ads_last_date);
                            defaults to last_date_str if None

    Returns:
        monthly:      {month: {gmv, settlement, orders, returns}}
        skus:         {sku_id: {name, orders, returns, gmv, settlement}}
        monthly_ads:  {month: ad_spend_float}  -- empty if no Ads sheet
        pay_new_last: str
        ads_new_last: str
    """
    if ads_last_date_str is None:
        ads_last_date_str = last_date_str

    last_date     = datetime.strptime(last_date_str,     '%Y-%m-%d').date()
    ads_last_date = datetime.strptime(ads_last_date_str, '%Y-%m-%d').date()

    xl = pd.ExcelFile(path)
    sheet_names = xl.sheet_names

    # ── Find the orders sheet ─────────────────────────────────────────────────
    orders_sheet = next(
        (s for s in sheet_names
         if s.lower() in ('orders', 'order') or
            ('order' in s.lower() and 'gst' not in s.lower() and 'ads' not in s.lower())),
        sheet_names[0]  # fallback: first sheet
    )

    # ── Process orders data ───────────────────────────────────────────────────
    df = xl.parse(orders_sheet, header=[0, 1])

    dates    = pd.to_datetime(df.iloc[:, 55], errors='coerce').dt.date
    skus_raw = df.iloc[:, 58].astype(str)
    sale_amt = pd.to_numeric(df.iloc[:, 9],  errors='coerce').fillna(0)
    sett_amt = pd.to_numeric(df.iloc[:, 3],  errors='coerce').fillna(0)
    ret_type = df.iloc[:, 62].astype(str)

    valid = dates.notna()
    df2   = pd.DataFrame({
        '_dt': dates[valid], 'sku': skus_raw[valid], 'sale': sale_amt[valid],
        'sett': sett_amt[valid], 'ret': ret_type[valid]
    })
    df_new   = df2[df2['_dt'] > last_date]
    pay_new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  FK Payments (orders): {len(df_new)} new rows "
          f"({df_new['_dt'].min() if len(df_new) else 'N/A'} to "
          f"{df_new['_dt'].max() if len(df_new) else 'N/A'}), "
          f"skipping {len(df2) - len(df_new)}")

    monthly = {}
    skus    = {}

    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if not mk:
            continue
        sale = float(row['sale'])
        sett = float(row['sett'])
        is_return = row['ret'] in ('Customer Return', 'Logistics Return')
        raw_sku = str(row['sku']).strip()
        sid, sname = fk_sku_id(raw_sku)

        m = monthly.setdefault(mk, {'gmv': 0, 'settlement': 0, 'orders': 0, 'returns': 0})
        s = skus.setdefault(sid, {'name': sname, 'type': '', 'orders': 0, 'returns': 0,
                                   'gmv': 0, 'settlement': 0})
        m['settlement'] += sett
        s['settlement'] += sett
        if sale > 0:
            m['gmv']    += sale
            m['orders'] += 1
            s['gmv']    += sale
            s['orders'] += 1
        if is_return:
            m['returns'] += 1
            s['returns'] += 1

    for m in monthly.values():
        m['gmv']        = round(m['gmv'], 2)
        m['settlement'] = round(m['settlement'], 2)
    for s in skus.values():
        s['gmv']        = round(s['gmv'], 2)
        s['settlement'] = round(s['settlement'], 2)

    # ── Process ads sheet (if present) ───────────────────────────────────────
    monthly_ads  = {}
    ads_new_last = ads_last_date

    ads_sheet = next(
        (s for s in sheet_names
         if s.lower() in ('ads', 'ad') or
            ('ads' in s.lower() and 'gst' not in s.lower())),
        None
    )

    if ads_sheet:
        try:
            # FK Ads sheet has 2-row headers: use header=[0,1] + positional access
            # col[1] = ('Payment Details', 'Payment Date')
            # col[6] = ('Transaction Summary', 'Wallet Redeem (Rs.)')
            df_ads = xl.parse(ads_sheet, header=[0, 1])
            # Keep as datetime64 for consistent pd.Timestamp comparison
            ad_dates = pd.to_datetime(df_ads.iloc[:, 1], errors='coerce')
            redeem   = pd.to_numeric(df_ads.iloc[:, 6], errors='coerce').fillna(0)

            valid_a    = ad_dates.notna()
            df_ads2    = pd.DataFrame({'_dt': ad_dates[valid_a], 'redeem': redeem[valid_a]})
            ads_cutoff = pd.Timestamp(ads_last_date)
            df_ads_new = df_ads2[df_ads2['_dt'] > ads_cutoff]
            ads_new_last = df_ads2['_dt'].dt.date.max() if len(df_ads2) else ads_last_date

            print(f"  FK Payments (ads):    {len(df_ads_new)} new rows, "
                  f"skipping {len(df_ads2) - len(df_ads_new)}")

            for _, row in df_ads_new.iterrows():
                mk = month_key(str(row['_dt'])[:10])
                if mk:
                    monthly_ads[mk] = monthly_ads.get(mk, 0) + abs(float(row['redeem']))
            monthly_ads = {k: round(v, 2) for k, v in monthly_ads.items()}

        except Exception as e:
            print(f"  FK Payments (ads sheet): error - {e}")

    # Log GST sheet if present
    gst_sheet = next((s for s in sheet_names if 'gst' in s.lower()), None)
    if gst_sheet:
        try:
            df_gst = xl.parse(gst_sheet)
            print(f"  FK Payments (GST):    {len(df_gst)} rows (logged only, not stored)")
        except Exception:
            pass

    return monthly, skus, monthly_ads, str(pay_new_last), str(ads_new_last)

# ─── Flipkart Ads ─────────────────────────────────────────────────────────────

def process_fk_ads(path, last_date_str):
    """Returns monthly: {month: ad_spend} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0])

    # Find payment date and wallet redeem columns
    date_col   = next((c for c in df.columns if 'Payment Date' in str(c)), None)
    redeem_col = next((c for c in df.columns if 'Wallet Redeem' in str(c)), None)

    if not date_col or not redeem_col:
        print("  FK Ads: Could not find required columns. Skipping.")
        return {}, last_date_str

    dates  = pd.to_datetime(df[date_col], errors='coerce').dt.date
    redeem = pd.to_numeric(df[redeem_col], errors='coerce').fillna(0)

    valid = dates.notna()
    df2 = pd.DataFrame({'_dt': dates[valid], 'redeem': redeem[valid]})
    df_new = df2[df2['_dt'] > last_date]
    new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  FK Ads: {len(df_new)} new rows, skipping {len(df2)-len(df_new)}")
    if len(df_new) == 0:
        return {}, str(new_last)

    monthly = {}
    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if mk:
            monthly[mk] = monthly.get(mk, 0) + abs(float(row['redeem']))

    return {k: round(v, 2) for k, v in monthly.items()}, str(new_last)

# ─── Flipkart Ads — Campaign Performance Report ──────────────────────────────

def process_fk_ads_campaign(path):
    """
    Process FK campaign performance report (Consolidate ad report format).
    Reads 'Overall Performance Report' sheet for per-SKU ad metrics.

    Columns used (by name — not positional):
        Sku Id                  -> SKU
        Views                   -> ad_views
        Clicks                  -> clicks
        Click Through Rate in % -> ctr
        Total converted units   -> conversions
        Ad Spend                -> ad_spend (total, not monthly)
        Total Revenue (Rs.)     -> ad_revenue

    Returns:
        skus:     {sku_id: {name, ad_views, clicks, ctr, conversions, ad_revenue, ad_spend}}
        total_spend: float  -- overall campaign spend in this report
    """
    xl = pd.ExcelFile(path)
    sheet = next(
        (s for s in xl.sheet_names if 'overall performance' in s.lower()),
        None
    )
    if not sheet:
        print("  FK Ads Campaign: 'Overall Performance Report' sheet not found")
        return {}, 0.0

    df = xl.parse(sheet)
    df.columns = [str(c).strip() for c in df.columns]

    sku_col      = next((c for c in df.columns if c.lower() == 'sku id' or 'sku id' in c.lower()), None)
    views_col    = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col   = next((c for c in df.columns if c.lower() == 'clicks'), None)
    ctr_col      = next((c for c in df.columns if 'click through rate' in c.lower()), None)
    conv_col     = next((c for c in df.columns if 'converted units' in c.lower()), None)
    spend_col    = next((c for c in df.columns if 'ad spend' in c.lower()), None)
    revenue_col  = next((c for c in df.columns if 'total revenue' in c.lower()), None)

    if not sku_col:
        print("  FK Ads Campaign: SKU Id column not found")
        return {}, 0.0

    skus = {}
    total_spend = 0.0

    for _, row in df.iterrows():
        raw_sku = str(row.get(sku_col, '')).strip()
        if not raw_sku or raw_sku.lower() in ('nan', 'none', ''):
            continue
        sid, sname = fk_sku_id(raw_sku)
        s = skus.setdefault(sid, {
            'name': sname, 'ad_views': 0, 'clicks': 0,
            'ctr': 0.0, 'conversions': 0, 'ad_revenue': 0.0, 'ad_spend': 0.0
        })
        if views_col:
            s['ad_views']   += int(float(row.get(views_col,  0) or 0))
        if clicks_col:
            s['clicks']     += int(float(row.get(clicks_col, 0) or 0))
        if conv_col:
            s['conversions']  += int(float(row.get(conv_col,    0) or 0))
        if spend_col:
            spend = float(row.get(spend_col, 0) or 0)
            s['ad_spend']   += spend
            total_spend     += spend
        if revenue_col:
            s['ad_revenue'] += float(row.get(revenue_col, 0) or 0)

    # Recalculate CTR from totals (more accurate than averaging per-row CTR)
    for s in skus.values():
        s['ctr']        = round(s['clicks'] / s['ad_views'] * 100, 2) if s['ad_views'] else 0
        s['ad_revenue'] = round(s['ad_revenue'], 2)
        s['ad_spend']   = round(s['ad_spend'], 2)

    print(f"  FK Ads Campaign: {len(skus)} SKUs, total spend = {round(total_spend, 2)}")
    return skus, round(total_spend, 2)


# ─── Flipkart Listings (OG vs Bahubali pricing pairs) ────────────────────────

def process_fk_listings(path):
    """
    Read Flipkart Master Listing file (XLS/XLSX) and build fk_pairs table.

    Row 0 of the sheet is a descriptions row (not data) — skip it.
    Only DJ- SKUs are processed. Bahubali vs OG classification is based on
    whether the Product Title contains 'Bahubali' (case-insensitive).

    Returns: list of dicts matching fk_pairs schema:
        [{'base', 'og_name', 'og_mrp', 'og_selling', 'og_settlement',
          'bahu_name', 'bahu_mrp', 'bahu_selling', 'bahu_settlement',
          'status', 'verdict'}, ...]
        status: 'pair' (both OG and Bahubali found) | 'solo' (only one variant)
    """
    import re

    try:
        xl = pd.ExcelFile(path)
        df = xl.parse(xl.sheet_names[0])   # header=0 → row 0 = column names
        df = df.iloc[1:].reset_index(drop=True)  # drop description row
    except Exception as e:
        print(f"  FK Listings: read error — {e}")
        return []

    # Identify columns (by name from header row)
    title_col = 'Product Title'
    sku_col   = 'Seller SKU Id'
    mrp_col   = 'MRP'
    sett_col  = 'Bank Settlement'
    sell_col  = 'Your Selling Price'

    # Filter to DJ- SKUs only
    dj = df[df[sku_col].astype(str).str.contains('DJ-', na=False)].copy()
    if dj.empty:
        print("  FK Listings: no DJ- SKUs found")
        return []

    # Extract base number (e.g. 'DJ-11' from 'DJ-11 BAHUBALI')
    def base_num(sku):
        m = re.search(r'(DJ-\d+)', str(sku))
        return m.group(1) if m else None

    dj['_base']    = dj[sku_col].apply(base_num)
    dj['_is_bahu'] = dj[title_col].astype(str).str.contains('Bahubali', case=False)

    pairs = {}
    for _, row in dj.iterrows():
        base = row['_base']
        if not base:
            continue
        p = pairs.setdefault(base, {})
        try:
            mrp  = float(row[mrp_col])  if pd.notna(row[mrp_col])  else 0
        except (ValueError, TypeError):
            mrp  = 0
        try:
            sell = float(row[sell_col]) if pd.notna(row[sell_col]) else 0
        except (ValueError, TypeError):
            sell = 0
        try:
            sett = float(row[sett_col]) if pd.notna(row[sett_col]) else 0
        except (ValueError, TypeError):
            sett = 0
        sku_str = str(row[sku_col])

        if row['_is_bahu']:
            # If multiple Bahubali variants for same base, keep first
            if 'bahu_name' not in p:
                p['bahu_name']       = sku_str
                p['bahu_mrp']        = mrp
                p['bahu_selling']    = sell
                p['bahu_settlement'] = sett
        else:
            # If multiple OG variants for same base, keep first
            if 'og_name' not in p:
                p['og_name']       = sku_str
                p['og_mrp']        = mrp
                p['og_selling']    = sell
                p['og_settlement'] = sett

    result = []
    for base, p in sorted(pairs.items()):
        has_og   = bool(p.get('og_name'))
        has_bahu = bool(p.get('bahu_name'))
        # verdict: Bahubali premium/discount vs OG, or 'solo' if no pair
        verdict = ''
        if has_og and has_bahu:
            diff = p['bahu_selling'] - p['og_selling']
            if diff > 0:
                verdict = f"Bahu +₹{int(diff)}"
            elif diff < 0:
                verdict = f"OG +₹{int(-diff)}"
            else:
                verdict = 'Same price'

        result.append({
            'base':            base,
            'og_name':         p.get('og_name', ''),
            'og_mrp':          p.get('og_mrp', 0),
            'og_selling':      p.get('og_selling', 0),
            'og_settlement':   p.get('og_settlement', 0),
            'bahu_name':       p.get('bahu_name', ''),
            'bahu_mrp':        p.get('bahu_mrp', 0),
            'bahu_selling':    p.get('bahu_selling', 0),
            'bahu_settlement': p.get('bahu_settlement', 0),
            'status':          'pair' if (has_og and has_bahu) else 'solo',
            'verdict':         verdict,
        })

    pairs_count = sum(1 for r in result if r['status'] == 'pair')
    print(f"  FK Listings: {len(result)} base SKUs, {pairs_count} OG/Bahubali pairs")
    return result


# ─── Flipkart Views ───────────────────────────────────────────────────────────

def _read_tabular(path, dtype=None):
    """Read CSV or XLSX transparently. Handles both native XLSX and CSV exports."""
    ext = Path(path).suffix.lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(path, dtype=dtype or {})
    else:
        return pd.read_csv(path, dtype=dtype or {}, encoding='utf-8', errors='replace')


def process_fk_views(path, last_date_str):
    """Returns skus: {sku_id: {views, clicks, sales, revenue, ctr}} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = _read_tabular(path, dtype={'Impression Date': str})

    df['_dt'] = pd.to_datetime(df['Impression Date'], errors='coerce').dt.date
    df = df[df['_dt'].notna()]
    df_new = df[df['_dt'] > last_date]
    new_last = df['_dt'].max() if len(df) else last_date

    print(f"  FK Views: {len(df_new)} new rows, skipping {len(df)-len(df_new)}")
    if len(df_new) == 0:
        return {}, str(new_last)

    skus = {}
    for sku_raw, grp in df_new.groupby('SKU Id'):
        sid, sname = fk_sku_id(str(sku_raw))
        s = skus.setdefault(sid, {'name': sname, 'ad_views': 0, 'clicks': 0,
                                   'sales': 0, 'ad_revenue': 0, 'ctr': 0})
        s['ad_views']   += int(grp['Product Views'].sum())
        s['clicks']     += int(grp['Product Clicks'].sum())
        s['sales']      += int(grp['Sales'].sum())
        s['ad_revenue'] += float(grp['Revenue'].sum())
        total_views = s['ad_views']
        s['ctr'] = round(s['clicks'] / total_views * 100, 2) if total_views else 0

    return skus, str(new_last)

# ─── Catalog ─────────────────────────────────────────────────────────────────

def process_catalog(path):
    """Returns {sku_id: stock_count} for Meesho catalog."""
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0])
    xl.close()
    df.columns = [str(c).strip() for c in df.columns]

    # Skip first row if it's a description row
    if 'Row identifier' in str(df.iloc[0, 0]):
        df = df.iloc[1:].reset_index(drop=True)

    style_col = next((c for c in df.columns if 'STYLE ID' in c.upper() or 'Style ID' in c), None)
    stock_col = next((c for c in df.columns if 'SYSTEM STOCK' in c.upper()), None)

    if not style_col or not stock_col:
        print(f"  Catalog: Could not find STYLE ID or SYSTEM STOCK columns. Found: {list(df.columns)}")
        return {}

    stocks = {}
    for _, row in df.iterrows():
        raw = str(row.get(style_col, '')).strip()
        cnt = row.get(stock_col, None)
        if pd.isna(cnt) or not raw or raw == 'nan':
            continue
        sid, _ = me_sku_id(raw)
        stocks[sid] = int(float(cnt))

    print(f"  Catalog: {len(stocks)} SKUs with stock data")
    return stocks

# ─── Flipkart Keywords ───────────────────────────────────────────────────────

def process_fk_keywords(path, last_date_str):
    """
    Process FK keyword performance CSV (attributed_keyword_views reports).

    Expected columns (flexible detection):
        keyword / search_term / keyword_text  -> keyword
        attributed_keyword_views / views      -> views
        clicks                                -> clicks
        attributed_orders / orders            -> orders
        attributed_revenue / revenue / gmv    -> revenue
        date / report_date                    -> date for deduplication

    Returns:
        keywords:      {keyword_str: {views, clicks, orders, revenue}}
        new_last_date: str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(path, dtype=str)

    # Normalise column names
    df.columns = [c.strip() for c in df.columns]

    # ── Date deduplication ────────────────────────────────────────────────────
    # Match any column that has 'date', 'week', or 'month' in its name
    date_col = next(
        (c for c in df.columns
         if any(k in c.lower() for k in ('date', 'week', 'month', 'report_date'))),
        None
    )
    if date_col:
        df['_dt'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
        df_valid  = df[df['_dt'].notna()]
        df_new    = df_valid[df_valid['_dt'] > last_date]
        new_last  = df_valid['_dt'].max() if len(df_valid) else last_date
        print(f"  FK Keywords: {len(df_new)} new rows (date col: {date_col!r}), "
              f"skipping {len(df_valid) - len(df_new)}")
    else:
        # No date column — process everything (no deduplication possible)
        df_new   = df
        new_last = last_date
        print(f"  FK Keywords: {len(df_new)} new rows (no date column found)")

    if len(df_new) == 0:
        return {}, str(new_last)

    # ── Detect columns ────────────────────────────────────────────────────────
    kw_col = next(
        (c for c in df_new.columns
         if any(k in c.lower() for k in ('keyword', 'search_term', 'query'))),
        None
    )
    # Views: prefer specific names; never match a date column
    views_col = next(
        (c for c in df_new.columns
         if c != date_col and
         any(k in c.lower() for k in (
             'attributed_keyword_views', 'keyword_views',
             'total_product_views', 'impressions'
         ))),
        None
    )
    clicks_col = next(
        (c for c in df_new.columns
         if c != date_col and 'click' in c.lower()),
        None
    )
    orders_col = next(
        (c for c in df_new.columns
         if c != date_col and
         any(k in c.lower() for k in ('attributed_orders', 'orders', 'units'))),
        None
    )
    revenue_col = next(
        (c for c in df_new.columns
         if c != date_col and
         any(k in c.lower() for k in ('revenue', 'gmv', 'sales_value'))),
        None
    )

    if not kw_col:
        print("  FK Keywords: keyword column not found. Skipping.")
        return {}, str(new_last)

    print(f"  FK Keywords: cols — keyword={kw_col!r}, views={views_col!r}, "
          f"clicks={clicks_col!r}, orders={orders_col!r}")

    # ── Aggregate by keyword (sum across all SKUs and dates) ──────────────────
    keywords = {}
    for _, row in df_new.iterrows():
        kw = str(row.get(kw_col, '')).strip()
        if not kw or kw.lower() in ('nan', 'none', ''):
            continue
        k = keywords.setdefault(kw, {'views': 0, 'clicks': 0, 'orders': 0, 'revenue': 0.0})
        if views_col:
            try:
                k['views'] += int(float(row.get(views_col, 0) or 0))
            except (ValueError, TypeError):
                pass
        if clicks_col:
            try:
                k['clicks'] += int(float(row.get(clicks_col, 0) or 0))
            except (ValueError, TypeError):
                pass
        if orders_col:
            try:
                k['orders'] += int(float(row.get(orders_col, 0) or 0))
            except (ValueError, TypeError):
                pass
        if revenue_col:
            try:
                k['revenue'] += float(row.get(revenue_col, 0) or 0)
            except (ValueError, TypeError):
                pass

    for k in keywords.values():
        k['revenue'] = round(k['revenue'], 2)

    print(f"  FK Keywords: {len(keywords)} unique keywords aggregated")
    return keywords, str(new_last)


# ─── Meesho Claims ────────────────────────────────────────────────────────────

def process_meesho_claims(file_path, last_date_str):
    """
    Process Meesho Seller Support ticket/claims CSV export.

    Expected columns (flexible detection):
        Order Number, Suborder Number/Sub Order No, Ticket ID, Ticket Status,
        Issue, Created Date, Last Update Date, Reopen Validity

    The 'Last Update Date' text field often contains a payment confirmation sentence:
        "Your claim payment of Rs 500 was done on 12-May-2026 with a transaction id: TXN123"
    We parse that with a regex to extract amount_recovered and transaction_id.

    Returns: list of claim dicts matching me_claims schema, new_last_date str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    try:
        df = pd.read_csv(file_path, dtype=str)
        df.columns = [c.strip() for c in df.columns]
    except Exception as e:
        print(f"  ME Claims: read error — {e}")
        return [], last_date_str

    # Column detection (flexible names)
    order_col    = next((c for c in df.columns if 'Order Number' in c
                         and 'Sub' not in c and 'Suborder' not in c), None)
    suborder_col = next((c for c in df.columns if 'Suborder' in c
                         or 'Sub Order' in c), None)
    ticket_col   = next((c for c in df.columns if 'Ticket ID' in c), None)
    status_col   = next((c for c in df.columns if 'Ticket Status' in c), None)
    created_col  = next((c for c in df.columns if 'Created Date' in c), None)
    issue_col    = next((c for c in df.columns if c.strip() == 'Issue'
                         or ('Issue' in c and 'Reopen' not in c)), None)
    last_upd_col = next((c for c in df.columns if 'Last Update' in c), None)
    reopen_col   = next((c for c in df.columns if 'Reopen' in c), None)

    if not ticket_col:
        print(f"  ME Claims: Ticket ID column not found. Columns: {list(df.columns)}")
        return [], last_date_str

    # Regex to extract payment info from Last Update text
    pay_pattern = re.compile(
        r'Your claim payment of Rs[\s.]*([\d,]+(?:\.\d+)?)\s*was done on\s*(.+?)'
        r'\s*with a transaction id[:\s]+(\S+)',
        re.IGNORECASE
    )

    # Date-based deduplication using Created Date
    if created_col:
        df['_dt'] = pd.to_datetime(df[created_col], errors='coerce').dt.date
        df_valid  = df[df['_dt'].notna()]
        df_new    = df_valid[df_valid['_dt'] > last_date]
        new_last  = df_valid['_dt'].max() if len(df_valid) else last_date
        print(f"  ME Claims: {len(df_new)} new rows, skipping {len(df_valid)-len(df_new)}")
    else:
        # No date column — process all rows (no deduplication)
        df_new   = df
        new_last = last_date
        print(f"  ME Claims: {len(df_new)} rows (no Created Date column — no deduplication)")

    rows = []
    for _, row in df_new.iterrows():
        order_id    = str(row.get(order_col,    '') if order_col    else '').strip()
        suborder_id = str(row.get(suborder_col, '') if suborder_col else '').strip()
        ticket_id   = str(row.get(ticket_col,   '') if ticket_col   else '').strip()
        status      = str(row.get(status_col,   '') if status_col   else '').strip()
        issue_type  = str(row.get(issue_col,    '') if issue_col    else '').strip()
        created     = str(row.get(created_col,  '') if created_col  else '').strip()
        last_upd    = str(row.get(last_upd_col, '') if last_upd_col else '').strip()
        reopen_val  = str(row.get(reopen_col,   '') if reopen_col   else '').strip()

        # Try to extract payment amount and transaction ID from last update text
        amount_recovered = ''
        transaction_id   = ''
        if last_upd:
            m = pay_pattern.search(last_upd)
            if m:
                amount_str = m.group(1).replace(',', '')
                try:
                    amount_recovered = str(round(float(amount_str), 2))
                except ValueError:
                    amount_recovered = amount_str
                transaction_id = m.group(3).strip()

        if not ticket_id or ticket_id in ('nan', 'None', ''):
            continue

        rows.append({
            'order_id':        order_id,
            'suborder_id':     suborder_id,
            'ticket_id':       ticket_id,
            'status':          status,
            'issue_type':      issue_type,
            'created_date':    created,
            'last_update':     last_upd[:200] if last_upd else '',  # cap long text
            'reopen_validity': reopen_val,
            'amount_recovered': amount_recovered,
            'transaction_id':  transaction_id,
        })

    print(f"  ME Claims: {len(rows)} claim records extracted")
    return rows, str(new_last)


def process_flipkart_claims(file_path, last_date_str):
    """
    Process Flipkart Seller Claims XLSX.

    Expects two sheets:
        'Seller Claims'        — manually filed claims
        'Auto-Approved Claims' — auto-approved claims

    Seller Claims columns (flexible detection):
        Claim ID, Incident ID, Order ID, Order Item ID, Source/Claim Type,
        Created At/Date, Updated At/Date, Status, Approved Amount,
        Not Approved Reason, Auto Claim Reason

    Returns: list of claim dicts matching fk_claims schema, new_last_date str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    try:
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
    except Exception as e:
        print(f"  FK Claims: read error — {e}")
        return [], last_date_str

    rows = []
    new_last = last_date

    # Process all sheets that have 'claim' in their name
    for sheet in sheet_names:
        if 'claim' not in sheet.lower():
            continue
        try:
            df = xl.parse(sheet, dtype=str)
            df.columns = [str(c).strip() for c in df.columns]
        except Exception as e:
            print(f"  FK Claims: could not parse sheet '{sheet}' — {e}")
            continue

        # Column detection
        claim_col    = next((c for c in df.columns if 'Claim ID' in c or c.lower() == 'claim id'), None)
        incident_col = next((c for c in df.columns if 'Incident' in c), None)
        order_col    = next((c for c in df.columns
                             if 'Order ID' in c and 'Item' not in c and 'Order Item' not in c), None)
        item_col     = next((c for c in df.columns if 'Order Item ID' in c or 'Item ID' in c), None)
        source_col   = next((c for c in df.columns
                             if 'Source' in c or 'Claim Type' in c or 'Type' in c), None)
        created_col  = next((c for c in df.columns if 'Created' in c), None)
        updated_col  = next((c for c in df.columns if 'Updated' in c or 'Modified' in c), None)
        status_col   = next((c for c in df.columns if 'Status' in c), None)
        amount_col   = next((c for c in df.columns
                             if 'Approved Amount' in c or 'Amount' in c), None)
        reason_col   = next((c for c in df.columns if 'Not Approved' in c), None)
        auto_col     = next((c for c in df.columns if 'Auto' in c and 'Reason' in c), None)

        # Date-based deduplication on Created At
        if created_col:
            df['_dt'] = pd.to_datetime(df[created_col], errors='coerce').dt.date
            df_valid  = df[df['_dt'].notna()]
            df_sheet  = df_valid[df_valid['_dt'] > last_date]
            sheet_max = df_valid['_dt'].max() if len(df_valid) else last_date
            if sheet_max > new_last:
                new_last = sheet_max
            skipped = len(df_valid) - len(df_sheet)
            print(f"  FK Claims ({sheet}): {len(df_sheet)} new rows, skipping {skipped}")
        else:
            df_sheet = df
            print(f"  FK Claims ({sheet}): {len(df_sheet)} rows (no date deduplication)")

        for _, row in df_sheet.iterrows():
            claim_id   = str(row.get(claim_col,    '') if claim_col    else '').strip()
            incident   = str(row.get(incident_col, '') if incident_col else '').strip()
            order_id   = str(row.get(order_col,    '') if order_col    else '').strip()
            item_id    = str(row.get(item_col,     '') if item_col     else '').strip()
            source     = str(row.get(source_col,   '') if source_col   else sheet).strip()
            created_at = str(row.get(created_col,  '') if created_col  else '').strip()
            updated_at = str(row.get(updated_col,  '') if updated_col  else '').strip()
            status     = str(row.get(status_col,   '') if status_col   else '').strip()
            amount_str = str(row.get(amount_col,   '') if amount_col   else '').strip()
            not_appr   = str(row.get(reason_col,   '') if reason_col   else '').strip()
            auto_rsn   = str(row.get(auto_col,     '') if auto_col     else '').strip()

            # Parse approved amount
            try:
                approved_amount = str(round(float(amount_str.replace(',', '')), 2)) \
                                  if amount_str not in ('', 'nan', 'None') else ''
            except (ValueError, TypeError):
                approved_amount = ''

            if not order_id or order_id in ('nan', 'None', ''):
                continue

            rows.append({
                'claim_id':           claim_id,
                'incident_id':        incident,
                'order_id':           order_id,
                'order_item_id':      item_id,
                'source':             source,
                'created_at':         created_at,
                'updated_at':         updated_at,
                'status':             status,
                'approved_amount':    approved_amount,
                'not_approved_reason': not_appr[:200] if not_appr else '',
                'auto_claim_reason':  auto_rsn[:200] if auto_rsn else '',
            })

    print(f"  FK Claims: {len(rows)} total claim records from {len(sheet_names)} sheet(s)")
    return rows, str(new_last)


def merge_claims(existing_rows, new_rows, key_col):
    """
    Merge claim rows: new rows replace existing ones by key_col value (e.g. ticket_id,
    claim_id). New keys are appended. Returns merged list sorted by key.
    """
    ex = {str(r.get(key_col, '')): dict(r) for r in existing_rows
          if r.get(key_col)}
    for r in new_rows:
        k = str(r.get(key_col, ''))
        if k:
            ex[k] = dict(r)
    return sorted(ex.values(), key=lambda r: r.get(key_col, ''))


# ─── Merge helpers ────────────────────────────────────────────────────────────

def merge_monthly(existing_rows, new_monthly, platform, new_sett=None, new_ads=None):
    """Merge new monthly data into existing rows list.
       new_monthly: {month: {gmv, orders, returns, settlement (optional)}}
       new_sett:    {month: settlement_float}
       new_ads:     {month: ad_spend_float}
    """
    # Build existing map
    ex = {r['month']: dict(r) for r in existing_rows}

    for mk, nd in new_monthly.items():
        r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                'gmv': 0, 'settlement': 0, 'orders': 0,
                                'returns': 0, 'ad_spend': 0})
        r['gmv']      = round(r.get('gmv', 0) + nd.get('gmv', 0), 2)
        r['orders']   = int(r.get('orders', 0)) + int(nd.get('orders', 0))
        r['returns']  = int(r.get('returns', 0)) + int(nd.get('returns', 0))
        if 'settlement' in nd:
            r['settlement'] = round(r.get('settlement', 0) + nd['settlement'], 2)

    if new_sett:
        for mk, sett in new_sett.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['settlement'] = round(r.get('settlement', 0) + sett, 2)

    if new_ads:
        for mk, ads in new_ads.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['ad_spend'] = round(r.get('ad_spend', 0) + ads, 2)

    # Sort by month
    return sorted(ex.values(), key=lambda r: r['month'])

def merge_me_skus(existing_rows, new_orders, new_returns, new_catalog):
    """Merge Meesho SKU data."""
    ex = {r['sku_id']: dict(r) for r in existing_rows}

    # Apply orders data
    for sid, nd in new_orders.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'total_orders': 0, 'delivered': 0, 'rto': 0, 'cust_returns': 0,
            'return_rate': 0, 'cust_ret_rate': 0, 'rto_rate': 0,
            'gmv': 0, 'avg_price': 0, 'incomplete': 0, 'wrong_product': 0, 'quality': 0
        })
        r['delivered']  = int(r.get('delivered', 0)) + int(nd['delivered'])
        r['rto']        = int(r.get('rto', 0)) + int(nd['rto'])
        r['gmv']        = round(r.get('gmv', 0) + nd['gmv'], 2)
        # Recalculate averages
        total = int(r['delivered']) + int(r['rto']) + int(r.get('cust_returns', 0))
        r['total_orders'] = total
        r['avg_price'] = round(r['gmv'] / r['delivered'], 2) if r['delivered'] else 0

    # Apply returns data
    for sid, nd in new_returns.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': sid, 'type': '',
            'total_orders': 0, 'delivered': 0, 'rto': 0, 'cust_returns': 0,
            'return_rate': 0, 'cust_ret_rate': 0, 'rto_rate': 0,
            'gmv': 0, 'avg_price': 0, 'incomplete': 0, 'wrong_product': 0, 'quality': 0
        })
        r['cust_returns']  = int(r.get('cust_returns', 0)) + int(nd['cust_returns'])
        r['incomplete']    = int(r.get('incomplete', 0)) + int(nd['incomplete'])
        r['wrong_product'] = int(r.get('wrong_product', 0)) + int(nd['wrong_product'])
        r['quality']       = int(r.get('quality', 0)) + int(nd['quality'])

    # Apply catalog stock
    for sid, stock in new_catalog.items():
        if sid in ex:
            ex[sid]['stock'] = stock

    # Recalculate rates
    for sid, r in ex.items():
        total = int(r.get('delivered', 0)) + int(r.get('rto', 0)) + int(r.get('cust_returns', 0))
        r['total_orders'] = total
        if total:
            r['rto_rate']      = round(int(r.get('rto', 0)) / total * 100, 2)
            r['cust_ret_rate'] = round(int(r.get('cust_returns', 0)) / total * 100, 2)
            r['return_rate']   = round((int(r.get('rto', 0)) + int(r.get('cust_returns', 0))) / total * 100, 2)
        else:
            r['rto_rate'] = r['cust_ret_rate'] = r['return_rate'] = 0

    return sorted(ex.values(), key=lambda r: -r.get('gmv', 0))

def merge_fk_skus(existing_rows, new_payments, new_views):
    """Merge FK SKU data."""
    ex = {r['sku_id']: dict(r) for r in existing_rows}

    for sid, nd in new_payments.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'mrp': 0, 'selling': 0, 'settlement': 0, 'stock': 0,
            'ctr': 0, 'ad_revenue': 0, 'conversions': 0, 'ad_views': 0
        })
        r['orders']     = int(r.get('orders', 0)) + int(nd.get('orders', 0))
        r['returns']    = int(r.get('returns', 0)) + int(nd.get('returns', 0))
        r['gmv']        = round(r.get('gmv', 0) + nd.get('gmv', 0), 2)
        r['settlement'] = round(r.get('settlement', 0) + nd.get('settlement', 0), 2)
        r['conversions']= int(r.get('conversions', 0)) + int(nd.get('orders', 0))

    for sid, nd in new_views.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'mrp': 0, 'selling': 0, 'settlement': 0, 'stock': 0,
            'ctr': 0, 'ad_revenue': 0, 'conversions': 0, 'ad_views': 0
        })
        r['ad_views']   = int(r.get('ad_views', 0)) + int(nd.get('ad_views', 0))
        r['ad_revenue'] = round(r.get('ad_revenue', 0) + nd.get('ad_revenue', 0), 2)
        total_views = r['ad_views']
        clicks = int(r.get('clicks', 0)) + int(nd.get('clicks', 0))
        r['clicks'] = clicks
        r['ctr'] = round(clicks / total_views * 100, 2) if total_views else 0

    return sorted(ex.values(), key=lambda r: -r.get('gmv', 0))

def build_return_reasons(existing_rows, new_reasons):
    """Merge return reason counts and compute percentages."""
    ex = {r['reason']: int(r.get('count', 0)) for r in existing_rows}
    for reason, cnt in new_reasons.items():
        ex[reason] = ex.get(reason, 0) + cnt
    total = sum(ex.values())
    rows = []
    for reason, cnt in sorted(ex.items(), key=lambda x: -x[1]):
        rows.append({'reason': reason, 'count': cnt, 'pct': round(cnt/total*100, 1) if total else 0})
    return rows

def merge_fk_keywords(existing_rows, new_keywords):
    """Merge FK keyword performance data, accumulating counts and recalculating rates."""
    ex = {r['keyword']: dict(r) for r in existing_rows}

    for kw, nd in new_keywords.items():
        r = ex.setdefault(kw, {
            'keyword': kw, 'views': 0, 'clicks': 0,
            'orders': 0, 'revenue': 0.0, 'ctr': 0.0, 'conversion_rate': 0.0
        })
        r['views']   = int(r.get('views',   0)) + int(nd['views'])
        r['clicks']  = int(r.get('clicks',  0)) + int(nd['clicks'])
        r['orders']  = int(r.get('orders',  0)) + int(nd['orders'])
        r['revenue'] = round(float(r.get('revenue', 0)) + float(nd['revenue']), 2)
        # Recalculate rates
        total_views  = r['views']
        r['ctr']             = round(r['clicks'] / total_views * 100, 2) if total_views else 0
        r['conversion_rate'] = round(r['orders'] / r['clicks'] * 100, 2) if r['clicks'] else 0

    return sorted(ex.values(), key=lambda r: -r.get('views', 0))


# ─── HTML Update ─────────────────────────────────────────────────────────────

def update_html_date(html_path, new_date):
    """Update EMBEDDED_DATA_DATE in index.html."""
    if not html_path.exists():
        return
    with open(html_path, encoding='utf-8') as f:
        content = f.read()
    import re
    updated = re.sub(
        r"const EMBEDDED_DATA_DATE = '[^']*';",
        f"const EMBEDDED_DATA_DATE = '{new_date}';",
        content
    )
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(updated)
    print(f"  Updated EMBEDDED_DATA_DATE to {new_date} in {html_path.name}")

def update_html_db_url(html_path, github_username, repo_name='rumee-dashboard'):
    """Replace Google Sheets URL with GitHub raw URL."""
    if not html_path.exists():
        return
    with open(html_path, encoding='utf-8') as f:
        content = f.read()
    raw_url = f'https://raw.githubusercontent.com/{github_username}/{repo_name}/main/rumee_db_v1.csv'
    # Replace the DB_URL construction line
    import re
    updated = re.sub(
        r"const DB_URL = [^;]+;",
        f"const DB_URL = '{raw_url}';",
        content
    )
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(updated)
    print(f"  Updated DB_URL to GitHub raw URL in {html_path.name}")

# ─── Archive ──────────────────────────────────────────────────────────────────

def archive_files(file_paths, archive_dir):
    """Move processed files to archive directory (copy+delete on Windows lock)."""
    archive_dir.mkdir(parents=True, exist_ok=True)
    for fp in file_paths:
        dest = archive_dir / fp.name
        try:
            shutil.move(str(fp), str(dest))
        except PermissionError:
            shutil.copy2(str(fp), str(dest))
            try:
                fp.unlink()
            except PermissionError:
                print(f"  WARNING: could not delete {fp.name} (file lock) — copied to archive")
                continue
        print(f"  Archived: {fp.name} -> {dest.relative_to(BASE_DIR)}")

# ─── Daily / Keywords Builders ───────────────────────────────────────────────

def _daily_window_start():
    """Return ISO date string for exactly 6 calendar months ago."""
    import calendar
    t   = date.today()
    m   = t.month - 6
    y   = t.year
    if m <= 0:
        m += 12
        y -= 1
    day = min(t.day, calendar.monthrange(y, m)[1])
    return date(y, m, day).isoformat()


def _read_me_orders_raw(path):
    """Read ME Orders CSV into a raw DataFrame (all rows, no date cutoff)."""
    try:
        df = pd.read_csv(path, dtype={'Order Date': str, 'Customer State': str})
        df['_dt'] = pd.to_datetime(df['Order Date'], errors='coerce').dt.date
        return df[df['_dt'].notna()].copy()
    except Exception as e:
        print(f"    build_me_daily: orders read error ({path.name}): {e}")
        return pd.DataFrame()


def _read_me_returns_raw(path):
    """Read ME Returns CSV (6-line header) into a raw DataFrame (all rows)."""
    try:
        with open(path, newline='', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
        # Find the header row (contains 'S No' and 'Product Name')
        header_idx = next(
            (i for i, ln in enumerate(lines)
             if ('"S No"' in ln or 'S No' in ln) and 'Product Name' in ln),
            6
        )
        import io
        df = pd.read_csv(io.StringIO(''.join(lines[header_idx:])))
        df.columns = [c.strip('"').strip() for c in df.columns]
        date_col = next((c for c in df.columns if 'Return Created Date' in c), None)
        sku_col  = next((c for c in df.columns if c == 'SKU'), 'SKU')
        reason_col     = next((c for c in df.columns if 'Detailed Return Reason' in c), None)
        sub_reason_col = next((c for c in df.columns if 'Return Reason' in c
                               and 'Detailed' not in c), None)
        if date_col:
            df['_dt'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
        else:
            return pd.DataFrame()
        df['_sku']    = df[sku_col].astype(str).str.strip('"').str.strip()
        df['_reason'] = df.apply(
            lambda r: (str(r.get(reason_col, '') or '').strip('"').strip()
                       if reason_col else '') or
                      (str(r.get(sub_reason_col, '') or '').strip('"').strip()
                       if sub_reason_col else ''),
            axis=1
        )
        return df[df['_dt'].notna()].copy()
    except Exception as e:
        print(f"    build_me_daily: returns read error ({path.name}): {e}")
        return pd.DataFrame()


def build_me_daily(orders_paths, returns_paths, window_start,
                   cutoff=None, skip_zero_fill=False):
    """
    Build me_daily rows from raw ME orders + returns files.
    Groups by Order Date + SKU. Applies rolling window (window_start to today).
    If cutoff is None, no upper cutoff (used by --generate-alltime).

    Returns list of row dicts matching me_daily schema.
    """
    from datetime import timedelta
    if not orders_paths:
        return []

    window_dt = datetime.strptime(window_start, '%Y-%m-%d').date()

    # Read and concat all orders files
    raw_dfs = [_read_me_orders_raw(p) for p in orders_paths]
    orders_df = pd.concat([d for d in raw_dfs if not d.empty], ignore_index=True)
    if orders_df.empty:
        return []

    # Apply window
    orders_df = orders_df[orders_df['_dt'] >= window_dt].copy()
    if orders_df.empty:
        return []

    status_col = 'Reason for Credit Entry'
    price_col  = 'Supplier Discounted Price (Incl GST and Commision)'
    sku_col    = 'SKU'
    state_col  = 'Customer State'

    orders_df['_sid']    = orders_df[sku_col].astype(str).apply(
        lambda x: me_sku_id(x.strip())[0])
    orders_df['_sname']  = orders_df[sku_col].astype(str).apply(
        lambda x: me_sku_id(x.strip())[1])
    orders_df['_status'] = orders_df[status_col].astype(str).str.strip()
    orders_df['_price']  = pd.to_numeric(
        orders_df[price_col], errors='coerce').fillna(0)
    if state_col in orders_df.columns:
        orders_df['_state'] = orders_df[state_col].astype(str).str.strip()
    else:
        orders_df['_state'] = ''

    # ── Group by date + sku_id ───────────────────────────────────────────────
    daily = {}
    for (dt, sid), grp in orders_df.groupby(['_dt', '_sid']):
        sname     = grp['_sname'].iloc[0]
        statuses  = grp['_status']
        delivered = int((statuses == 'DELIVERED').sum())
        rto       = int((statuses == 'RTO_COMPLETE').sum())
        cancelled = int(statuses.isin(['CANCELLED', 'LOST']).sum())
        gmv       = round(float(
            grp.loc[statuses == 'DELIVERED', '_price'].sum()), 2)
        daily[(str(dt), sid)] = {
            'date': str(dt), 'sku_id': sid, 'sku_name': sname,
            'orders_placed': len(grp),
            'delivered': delivered, 'rto': rto, 'cancelled': cancelled,
            'gmv': gmv,
            'returns_received': 0, 'top_return_reason': '', 'states': '',
        }

    # ── Top 3 states per (date, sku) ────────────────────────────────────────
    state_grp = orders_df[orders_df['_state'].str.len() > 0]
    for (dt, sid), grp in state_grp.groupby(['_dt', '_sid']):
        key = (str(dt), sid)
        if key in daily:
            top = grp['_state'].value_counts().head(3).index.tolist()
            daily[key]['states'] = '|'.join(top)

    # ── Merge returns ────────────────────────────────────────────────────────
    if returns_paths:
        ret_dfs = [_read_me_returns_raw(p) for p in returns_paths]
        ret_df  = pd.concat([d for d in ret_dfs if not d.empty], ignore_index=True)
        if not ret_df.empty:
            ret_df = ret_df[ret_df['_dt'] >= window_dt].copy()
            ret_df['_sid'] = ret_df['_sku'].apply(lambda x: me_sku_id(x)[0])
            for (dt, sid), grp in ret_df.groupby(['_dt', '_sid']):
                key = (str(dt), sid)
                if key not in daily:
                    continue
                daily[key]['returns_received'] = len(grp)
                top_r = (grp['_reason']
                         .replace('', pd.NA).dropna()
                         .value_counts().head(1).index.tolist())
                if top_r:
                    daily[key]['top_return_reason'] = top_r[0]

    # ── Zero-fill: add 0-order rows for every window day × active SKU ───────
    if not skip_zero_fill:
        active_skus = {}
        for (dt_str, sid), r in daily.items():
            active_skus[sid] = r['sku_name']

        cur = window_dt
        end = date.today()
        while cur <= end:
            dt_str = str(cur)
            for sid, sname in active_skus.items():
                key = (dt_str, sid)
                if key not in daily:
                    daily[key] = {
                        'date': dt_str, 'sku_id': sid, 'sku_name': sname,
                        'orders_placed': 0, 'delivered': 0, 'rto': 0,
                        'cancelled': 0, 'gmv': 0,
                        'returns_received': 0, 'top_return_reason': '',
                        'states': '',
                    }
            cur += timedelta(days=1)

    rows = sorted(daily.values(), key=lambda r: (r['date'], r['sku_id']))
    if rows:
        n_skus = len({r['sku_id'] for r in rows})
        print(f"    build_me_daily: {len(rows)} rows "
              f"({rows[0]['date']} to {rows[-1]['date']}, "
              f"{n_skus} SKUs)")
    return rows


def build_fk_daily(views_paths, window_start,
                   cutoff=None, skip_zero_fill=False):
    """
    Build fk_daily rows from raw FK Views CSVs.
    Groups by Impression Date + SKU Id. Applies rolling window.
    If cutoff is None, no upper cutoff (used by --generate-alltime).

    Returns list of row dicts matching fk_daily schema.
    """
    from datetime import timedelta
    if not views_paths:
        return []

    window_dt = datetime.strptime(window_start, '%Y-%m-%d').date()

    dfs = []
    for p in views_paths:
        try:
            df = _read_tabular(p, dtype={'Impression Date': str})
            dfs.append(df)
        except Exception as e:
            print(f"    build_fk_daily: read error ({p.name}): {e}")

    if not dfs:
        return []

    df = pd.concat(dfs, ignore_index=True)
    df['_dt'] = pd.to_datetime(df['Impression Date'], errors='coerce').dt.date
    df = df[df['_dt'].notna() & (df['_dt'] >= window_dt)].copy()
    if df.empty:
        return []

    df['_sid']   = df['SKU Id'].astype(str).apply(
        lambda x: fk_sku_id(x.strip())[0])
    df['_sname'] = df['SKU Id'].astype(str).apply(
        lambda x: fk_sku_id(x.strip())[1])

    for col in ['Product Views', 'Product Clicks', 'Sales', 'Revenue']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    daily = {}
    for (dt, sid), grp in df.groupby(['_dt', '_sid']):
        sname   = grp['_sname'].iloc[0]
        views   = int(grp['Product Views'].sum())
        clicks  = int(grp['Product Clicks'].sum())
        sales   = int(grp['Sales'].sum())
        revenue = round(float(grp['Revenue'].sum()), 2)
        ctr     = round(clicks  / views  * 100, 2) if views  else 0
        conv    = round(sales   / clicks * 100, 2) if clicks else 0
        daily[(str(dt), sid)] = {
            'date': str(dt), 'sku_id': sid, 'sku_name': sname,
            'views': views, 'clicks': clicks, 'sales': sales,
            'revenue': revenue, 'ctr': ctr, 'conversion_rate': conv,
        }

    # ── Zero-fill active SKUs ────────────────────────────────────────────────
    if not skip_zero_fill:
        active_skus = {r['sku_id']: r['sku_name'] for r in daily.values()}
        cur = window_dt
        end = date.today()
        while cur <= end:
            dt_str = str(cur)
            for sid, sname in active_skus.items():
                key = (dt_str, sid)
                if key not in daily:
                    daily[key] = {
                        'date': dt_str, 'sku_id': sid, 'sku_name': sname,
                        'views': 0, 'clicks': 0, 'sales': 0,
                        'revenue': 0, 'ctr': 0, 'conversion_rate': 0,
                    }
            cur += timedelta(days=1)

    rows = sorted(daily.values(), key=lambda r: (r['date'], r['sku_id']))
    if rows:
        n_skus = len({r['sku_id'] for r in rows})
        print(f"    build_fk_daily: {len(rows)} rows "
              f"({rows[0]['date']} to {rows[-1]['date']}, "
              f"{n_skus} SKUs)")
    return rows


def build_fk_keywords(keywords_paths):
    """
    Build fk_keywords rows grouped by month + SKU Id + keyword.
    Full history — no rolling window.

    Columns: month, sku_id, sku_name, keyword,
             total_views, impression_pct, attributed_views

    Returns list of row dicts sorted by month DESC, total_views DESC.
    """
    if not keywords_paths:
        return []

    dfs = []
    for p in keywords_paths:
        try:
            df = _read_tabular(p, dtype=str)
            df.columns = [c.strip() for c in df.columns]
            dfs.append(df)
        except Exception as e:
            print(f"    build_fk_keywords: read error ({p.name}): {e}")

    if not dfs:
        return []

    df = pd.concat(dfs, ignore_index=True)

    # Parse date → month
    if 'Impression Date' not in df.columns:
        print("    build_fk_keywords: 'Impression Date' column not found")
        return []
    df['_dt'] = pd.to_datetime(df['Impression Date'], errors='coerce')
    df = df[df['_dt'].notna()].copy()
    df['_month'] = df['_dt'].dt.strftime('%Y-%m')

    # SKU mapping
    if 'SKU Id' not in df.columns:
        print("    build_fk_keywords: 'SKU Id' column not found")
        return []
    df['_sid']   = df['SKU Id'].astype(str).apply(lambda x: fk_sku_id(x.strip())[0])
    df['_sname'] = df['SKU Id'].astype(str).apply(lambda x: fk_sku_id(x.strip())[1])

    # Numeric cols
    for col in ['total_product_views', 'keyword_impression_percentage',
                'attributed_keyword_views']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Keyword column
    if 'Keyword' not in df.columns:
        print("    build_fk_keywords: 'Keyword' column not found")
        return []

    rows = []
    grp_cols = ['_month', '_sid', 'Keyword']
    for (month, sid, kw), grp in df.groupby(grp_cols):
        sname   = grp['_sname'].iloc[0]
        tv      = int(grp['total_product_views'].sum()) \
                  if 'total_product_views' in grp.columns else 0
        imp_pct = round(float(grp['keyword_impression_percentage'].mean()), 4) \
                  if 'keyword_impression_percentage' in grp.columns else 0
        attr_v  = int(grp['attributed_keyword_views'].sum()) \
                  if 'attributed_keyword_views' in grp.columns else 0
        rows.append({
            'month': month, 'sku_id': sid, 'sku_name': sname,
            'keyword': str(kw),
            'total_views': tv, 'impression_pct': imp_pct,
            'attributed_views': attr_v,
        })

    # Sort: month DESC, then attributed_views DESC within each month
    rows.sort(key=lambda r: (r['month'], r['attributed_views']), reverse=True)

    unique_kw  = len({r['keyword'] for r in rows})
    unique_sku = len({r['sku_id'] for r in rows})
    print(f"    build_fk_keywords: {len(rows)} rows "
          f"({unique_sku} SKUs × {unique_kw} keywords, "
          f"{len({r['month'] for r in rows})} months)")
    return rows


# ─── Generate All-Time ────────────────────────────────────────────────────────

def _run_generate_alltime(db, args):
    """
    Generate rumee_db_alltime.csv with full-history daily tables.
    Does NOT touch rumee_db_summary.csv, rumee_db_daily.csv, or rumee_db_v1.csv.
    """
    # ── Rate limit: minimum 12 h between requests ────────────────────────────
    flag_path = BASE_DIR / 'request_alltime.flag'
    if flag_path.exists():
        content = flag_path.read_text().strip()
        try:
            from datetime import timezone as _tz
            requested_at = datetime.fromisoformat(content.replace('Z', '+00:00'))
            hours_since  = (datetime.now(_tz.utc) - requested_at).total_seconds() / 3600
            if hours_since < 12:
                print(f"Rate limit: all-time data was last requested {hours_since:.1f}h ago. "
                      f"Minimum 12h between requests.")
                return
        except (ValueError, TypeError):
            pass  # unparseable timestamp — proceed anyway

    print("\n  [--generate-alltime] Building full-history daily tables...")

    # Collect raw files — temporarily ignore processed_file cache
    orig_config = list(db.get('config', []))
    db['config'] = [r for r in orig_config
                    if not str(r.get('key', '')).startswith('processed_file:')]

    source_files = []
    if args.source == 'drive':
        try:
            from drive_connector import fetch_new_files, test_auth
            test_auth()
            results = fetch_new_files(db)
            source_files = results if results else _scan_local_files()
        except ImportError as e:
            print(f"\n  ERROR: {e}")
            sys.exit(1)
        except FileNotFoundError as e:
            print(f"\n  ERROR: Drive credentials not found — {e}")
            sys.exit(1)
        except Exception as e:
            print(f"\n  ERROR: Drive authentication failed — {e}")
            sys.exit(1)
    else:
        source_files = _scan_local_files()

    # Restore config (don't save cache changes)
    db['config'] = orig_config

    if not source_files:
        print("  No files found for alltime generation.")
        return

    source_files.sort(key=lambda x: x[0].name)

    # Collect paths by type
    me_orders_paths  = []
    me_returns_paths = []
    fk_views_paths   = []

    for fp, ft_hint in source_files:
        ft = detect_file_type(fp)
        if ft == 'UNKNOWN' and ft_hint:
            ft = ft_hint
        if ft == 'ME_ORDERS':
            me_orders_paths.append(fp)
        elif ft == 'ME_RETURNS':
            me_returns_paths.append(fp)
        elif ft == 'FK_VIEWS':
            fk_views_paths.append(fp)

    # Use earliest actual data date as window start; skip zero-fill for alltime
    EPOCH = '1970-01-01'

    fk_alltime = build_fk_daily(fk_views_paths, EPOCH, skip_zero_fill=True)
    me_alltime = build_me_daily(me_orders_paths, me_returns_paths, EPOCH,
                                skip_zero_fill=True)

    # Write alltime CSV
    with open(DB_ALLTIME_PATH, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        # fk_daily_alltime
        cols = _DAILY_SCHEMAS['fk_daily']
        w.writerow(['__table__'] + cols)
        for rec in fk_alltime:
            w.writerow(['fk_daily_alltime'] + [rec.get(c, '') for c in cols])
        # me_daily_alltime
        cols = _DAILY_SCHEMAS['me_daily']
        w.writerow(['__table__'] + cols)
        for rec in me_alltime:
            w.writerow(['me_daily_alltime'] + [rec.get(c, '') for c in cols])

    all_rows  = fk_alltime + me_alltime
    all_dates = [r['date'] for r in all_rows if r.get('date')]
    d_min     = min(all_dates) if all_dates else 'N/A'
    d_max     = max(all_dates) if all_dates else 'N/A'
    print(f"\n  All-time data generated: {len(all_rows)} rows "
          f"covering {d_min} to {d_max}")
    print(f"  Written to: {DB_ALLTIME_PATH.name}")

    # ── Send "all-time data ready" email ─────────────────────────────────────
    if getattr(args, 'email', False):
        gmail_user = os.environ.get('GMAIL_USER')
        gmail_pass = os.environ.get('GMAIL_APP_PASSWORD')
        if gmail_user and gmail_pass:
            try:
                subject = "Rumee — All-Time Data Ready"
                body = f"""
All-Time Data Generated
Date: {TODAY}

Coverage:   {d_min} → {d_max}
Total rows: {len(all_rows):,}  (FK: {len(fk_alltime):,}  |  Meesho: {len(me_alltime):,})

Open the dashboard and click "Load All-Time Data" to view.

Dashboard:  https://rumeein.github.io/rumee-dashboard/
Repository: https://github.com/Rumeein/rumee-dashboard
"""
                msg = MIMEMultipart()
                msg['Subject'] = subject
                msg['From']    = gmail_user
                msg['To']      = 'rumeein@gmail.com'
                msg.attach(MIMEText(body, 'plain'))
                with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                    server.login(gmail_user, gmail_pass)
                    server.send_message(msg)
                print("All-time ready email sent to rumeein@gmail.com")
            except Exception as e:
                print(f"Email failed: {e}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description='Rumee Dashboard Pipeline — process seller export files into DB'
    )
    parser.add_argument(
        '--source', choices=['local', 'drive'], default='local',
        help='Data source: local new_data/ folder (default) or Google Drive'
    )
    parser.add_argument(
        '--reset-db', action='store_true',
        help='Full clean slate: clear all data tables, reset last-date cutoffs to 1970-01-01, '
             'and clear Drive file-processed cache so all files are re-downloaded and reprocessed'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Detect and process files but do NOT save DB, update HTML, or archive'
    )
    parser.add_argument(
        '--generate-alltime', action='store_true',
        help='(future) Generate alltime data snapshot after processing'
    )
    parser.add_argument(
        '--email', action='store_true',
        help='Send summary email to rumeein@gmail.com after successful processing '
             '(requires GMAIL_USER and GMAIL_APP_PASSWORD env vars)'
    )
    return parser.parse_args()


def _scan_local_files():
    """Return [(path, None)] for every CSV/XLSX in new_data/."""
    NEW_DATA.mkdir(exist_ok=True)
    files = [
        f for f in NEW_DATA.iterdir()
        if f.is_file() and f.suffix.lower() in ('.csv', '.xlsx', '.xls')
    ]
    return [(f, None) for f in sorted(files)]


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    print(f"\n{'='*60}")
    print(f"  Rumee Dashboard Pipeline -- {TODAY}")
    if args.dry_run:
        print("  [DRY RUN] -- DB will NOT be saved")
    print(f"{'='*60}")

    # ── --generate-alltime: separate path, exits early ───────────────────────
    if args.generate_alltime:
        db = load_db(DB_PATH) or {}
        _run_generate_alltime(db, args)
        return

    # ── Load existing DB ──────────────────────────────────────────────────────
    db = load_db(DB_PATH)
    if not db:
        print("\n  No existing DB -- creating fresh database.")
        db = {
            'config': [], 'fk_monthly': [], 'me_monthly': [],
            'fk_skus': [], 'me_skus': [], 'me_return_reasons': [],
            'fk_pairs': [], 'az_monthly': [], 'fk_keywords': [],
            'me_claims': [], 'fk_claims': [],
        }

    # ── Optional reset ────────────────────────────────────────────────────────
    if args.reset_db:
        print("\n  [--reset-db] Full clean slate...")
        # 1. Clear all data tables
        for t in ['fk_monthly', 'me_monthly', 'fk_skus', 'me_skus',
                  'me_return_reasons', 'fk_pairs', 'az_monthly', 'fk_keywords',
                  'me_claims', 'fk_claims']:
            db[t] = []
        # 2. Reset all *_last_date cutoffs → 1970-01-01 (process ALL historical rows)
        last_date_keys = [
            'me_orders_last_date', 'me_returns_last_date', 'me_payments_last_date',
            'me_ads_last_date', 'fk_payments_last_date', 'fk_ads_last_date',
            'fk_views_last_date', 'fk_keywords_last_date',
            'me_claims_last_date', 'fk_claims_last_date',
            'fk_listings_last_date', 'me_catalog_last_date',
        ]
        for k in last_date_keys:
            set_config(db, k, '1970-01-01')
        # 3. Remove all processed_file:* keys (so Drive re-downloads everything)
        db['config'] = [r for r in db.get('config', [])
                        if not str(r.get('key', '')).startswith('processed_file:')]
        # 4. Also wipe daily + keywords CSVs so they rebuild from scratch
        for p in [DB_DAILY_PATH, DB_KEYWORDS_PATH]:
            if p.exists():
                p.unlink()
        print(f"  Cleared data tables, reset {len(last_date_keys)} date cutoffs, "
              f"cleared Drive file cache.")

    # ── Find files ────────────────────────────────────────────────────────────
    source_files = []
    drive_paths  = set()   # Paths that came from Drive (need marking + temp cleanup)

    if args.source == 'drive':
        try:
            from drive_connector import fetch_new_files, test_auth
            print(f"\n  Verifying Drive credentials...")
            test_auth()   # raises immediately if credentials are missing or invalid
            print(f"  Drive: authenticated successfully")
            print(f"\n  Scanning Google Drive folders...")
            drive_results = fetch_new_files(db)
            if drive_results:
                source_files = drive_results
                drive_paths  = {fp for fp, _ in drive_results}
            else:
                print("  Drive: no new files found.")
                source_files = _scan_local_files()
        except ImportError as e:
            print(f"\n  ERROR: {e}")
            sys.exit(1)
        except FileNotFoundError as e:
            print(f"\n  ERROR: Drive credentials not found — {e}")
            print("  Set GOOGLE_DRIVE_CREDENTIALS env var or place credentials.json in project root.")
            sys.exit(1)
        except Exception as e:
            print(f"\n  ERROR: Drive authentication failed — {e}")
            sys.exit(1)
    else:
        source_files = _scan_local_files()

    if not source_files:
        print("\n  No files found. Drop export files in new_data/ and re-run.")
        return

    # Sort files so older monthly files (01_2026, 02_2026...) come before newer ones.
    # This ensures date-cutoff deduplication doesn't accidentally skip historical data
    # when multiple monthly files are processed in a single run.
    source_files.sort(key=lambda x: x[0].name)

    # ── Detect file types ─────────────────────────────────────────────────────
    print(f"\n  Found {len(source_files)} file(s):")
    typed = {}
    for (fp, ft_hint) in source_files:
        ft = detect_file_type(fp)
        if ft == 'UNKNOWN' and ft_hint:
            ft = ft_hint   # Trust Drive folder hint when sniff fails
        typed[fp] = ft
        print(f"    {fp.name:50s} = {ft}")

    # ── Last-processed dates ──────────────────────────────────────────────────
    me_orders_last    = get_config(db, 'me_orders_last_date')
    me_returns_last   = get_config(db, 'me_returns_last_date')
    me_payments_last  = get_config(db, 'me_payments_last_date')
    me_ads_last       = get_config(db, 'me_ads_last_date')
    fk_payments_last  = get_config(db, 'fk_payments_last_date')
    fk_ads_last       = get_config(db, 'fk_ads_last_date')
    fk_views_last     = get_config(db, 'fk_views_last_date')
    fk_keywords_last  = get_config(db, 'fk_keywords_last_date')
    me_claims_last    = get_config(db, 'me_claims_last_date')
    fk_claims_last    = get_config(db, 'fk_claims_last_date')

    processed_files = []

    # ── Accumulators ──────────────────────────────────────────────────────────
    me_orders_monthly = {}
    me_orders_skus    = {}
    me_sett_monthly   = {}
    me_ads_monthly    = {}
    me_return_skus    = {}
    me_return_reasons = {}
    me_catalog        = {}
    fk_pay_monthly    = {}
    fk_pay_skus       = {}
    fk_ads_monthly    = {}
    fk_views_skus     = {}
    fk_keywords_data  = {}
    fk_listings_pairs = []   # fk_pairs built from Listing file — replaces existing
    me_claims_rows    = []   # ME claims ticket rows (merged by ticket_id)
    fk_claims_rows    = []   # FK claims rows (merged by claim_id / order_id)

    # Path collectors for daily / keywords builders (parallel to existing flow)
    me_orders_paths   = []   # raw ME Orders files for build_me_daily
    me_returns_paths  = []   # raw ME Returns files for build_me_daily
    fk_views_paths    = []   # raw FK Views files for build_fk_daily
    fk_keywords_paths = []   # raw FK Keywords files for build_fk_keywords

    # ── Process each file ─────────────────────────────────────────────────────
    for fp, ft in typed.items():
        print(f"\n  Processing: {fp.name} ({ft})")

        if ft == 'ME_ORDERS':
            me_orders_paths.append(fp)          # collect for build_me_daily
            m, s, new_last = process_meesho_orders(fp, me_orders_last)
            me_orders_monthly.update(m)
            for sid, nd in s.items():
                if sid in me_orders_skus:
                    me_orders_skus[sid]['delivered'] += nd['delivered']
                    me_orders_skus[sid]['rto']       += nd['rto']
                    me_orders_skus[sid]['gmv']       += nd['gmv']
                else:
                    me_orders_skus[sid] = nd
            if new_last > me_orders_last:
                me_orders_last = new_last
                set_config(db, 'me_orders_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'ME_RETURNS':
            me_returns_paths.append(fp)         # collect for build_me_daily
            sr, reasons, new_last = process_meesho_returns(fp, me_returns_last)
            for sid, nd in sr.items():
                if sid in me_return_skus:
                    for k in nd:
                        me_return_skus[sid][k] = me_return_skus[sid].get(k, 0) + nd[k]
                else:
                    me_return_skus[sid] = dict(nd)
            for r, c in reasons.items():
                me_return_reasons[r] = me_return_reasons.get(r, 0) + c
            if new_last > me_returns_last:
                me_returns_last = new_last
                set_config(db, 'me_returns_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'ME_PAYMENTS':
            # Returns 4-tuple: (monthly_sett, monthly_ads, pay_new_last, ads_new_last)
            m, m_ads, pay_new_last, ads_new_last = process_meesho_payments(
                fp, me_payments_last, me_ads_last
            )
            for mk, sett in m.items():
                me_sett_monthly[mk] = me_sett_monthly.get(mk, 0) + sett
            for mk, ads in m_ads.items():
                me_ads_monthly[mk] = me_ads_monthly.get(mk, 0) + ads
            if pay_new_last > me_payments_last:
                me_payments_last = pay_new_last
                set_config(db, 'me_payments_last_date', pay_new_last)
            if m_ads and ads_new_last > me_ads_last:
                me_ads_last = ads_new_last
                set_config(db, 'me_ads_last_date', ads_new_last)
            processed_files.append(fp)

        elif ft == 'ME_ADS':
            m, new_last = process_meesho_ads(fp, me_ads_last)
            for mk, ads in m.items():
                me_ads_monthly[mk] = me_ads_monthly.get(mk, 0) + ads
            if new_last > me_ads_last:
                me_ads_last = new_last
                set_config(db, 'me_ads_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_PAYMENTS':
            # Returns 5-tuple: (monthly, skus, monthly_ads, pay_new_last, ads_new_last)
            m, s, m_ads, pay_new_last, ads_new_last = process_fk_payments(
                fp, fk_payments_last, fk_ads_last
            )
            for mk, nd in m.items():
                if mk in fk_pay_monthly:
                    for k in nd:
                        fk_pay_monthly[mk][k] = fk_pay_monthly[mk].get(k, 0) + nd[k]
                else:
                    fk_pay_monthly[mk] = dict(nd)
            for sid, nd in s.items():
                if sid in fk_pay_skus:
                    for k in nd:
                        if isinstance(nd[k], (int, float)):
                            fk_pay_skus[sid][k] = fk_pay_skus[sid].get(k, 0) + nd[k]
                else:
                    fk_pay_skus[sid] = dict(nd)
            for mk, ads in m_ads.items():
                fk_ads_monthly[mk] = fk_ads_monthly.get(mk, 0) + ads
            if pay_new_last > fk_payments_last:
                fk_payments_last = pay_new_last
                set_config(db, 'fk_payments_last_date', pay_new_last)
            if m_ads and ads_new_last > fk_ads_last:
                fk_ads_last = ads_new_last
                set_config(db, 'fk_ads_last_date', ads_new_last)
            processed_files.append(fp)

        elif ft == 'FK_ADS':
            m, new_last = process_fk_ads(fp, fk_ads_last)
            for mk, ads in m.items():
                fk_ads_monthly[mk] = fk_ads_monthly.get(mk, 0) + ads
            if new_last > fk_ads_last:
                fk_ads_last = new_last
                set_config(db, 'fk_ads_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_ADS_CAMPAIGN':
            camp_skus, _ = process_fk_ads_campaign(fp)
            # Merge campaign ad-performance data into fk_views_skus accumulator
            # (same merge path as FK_VIEWS — updates ad_views, ctr, ad_revenue, conversions)
            for sid, nd in camp_skus.items():
                if sid in fk_views_skus:
                    for k in ('ad_views', 'clicks', 'conversions'):
                        fk_views_skus[sid][k] = fk_views_skus[sid].get(k, 0) + nd.get(k, 0)
                    fk_views_skus[sid]['ad_revenue'] = round(
                        float(fk_views_skus[sid].get('ad_revenue', 0))
                        + float(nd.get('ad_revenue', 0)), 2
                    )
                else:
                    fk_views_skus[sid] = dict(nd)
            processed_files.append(fp)

        elif ft == 'FK_VIEWS':
            fk_views_paths.append(fp)           # collect for build_fk_daily
            s, new_last = process_fk_views(fp, fk_views_last)
            for sid, nd in s.items():
                if sid in fk_views_skus:
                    for k in ('ad_views', 'clicks', 'sales', 'ad_revenue'):
                        fk_views_skus[sid][k] = fk_views_skus[sid].get(k, 0) + nd.get(k, 0)
                else:
                    fk_views_skus[sid] = dict(nd)
            if new_last > fk_views_last:
                fk_views_last = new_last
                set_config(db, 'fk_views_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_KEYWORDS':
            fk_keywords_paths.append(fp)        # collect for build_fk_keywords
            kw, new_last = process_fk_keywords(fp, fk_keywords_last)
            for kw_name, nd in kw.items():
                if kw_name in fk_keywords_data:
                    for k in ('views', 'clicks', 'orders'):
                        fk_keywords_data[kw_name][k] = (
                            fk_keywords_data[kw_name].get(k, 0) + nd.get(k, 0)
                        )
                    fk_keywords_data[kw_name]['revenue'] = round(
                        float(fk_keywords_data[kw_name].get('revenue', 0))
                        + float(nd.get('revenue', 0)), 2
                    )
                else:
                    fk_keywords_data[kw_name] = dict(nd)
            if new_last > fk_keywords_last:
                fk_keywords_last = new_last
                set_config(db, 'fk_keywords_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_LISTINGS':
            pairs = process_fk_listings(fp)
            if pairs:
                fk_listings_pairs = pairs  # full replace — listing file is master data
                set_config(db, 'fk_listings_last_date', TODAY)
            processed_files.append(fp)

        elif ft == 'CATALOG':
            me_catalog = process_catalog(fp)
            if me_catalog:
                set_config(db, 'me_catalog_last_date', TODAY)
            processed_files.append(fp)

        elif ft == 'ME_CLAIMS':
            new_rows, new_last = process_meesho_claims(fp, me_claims_last)
            me_claims_rows.extend(new_rows)
            if new_last > me_claims_last:
                me_claims_last = new_last
                set_config(db, 'me_claims_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_CLAIMS':
            new_rows, new_last = process_flipkart_claims(fp, fk_claims_last)
            fk_claims_rows.extend(new_rows)
            if new_last > fk_claims_last:
                fk_claims_last = new_last
                set_config(db, 'fk_claims_last_date', new_last)
            processed_files.append(fp)

        else:
            print(f"  UNKNOWN file type -- skipping {fp.name}")

    if not processed_files:
        print("\n  No files were processed successfully.")
        return

    # ── Mark Drive files as processed ─────────────────────────────────────────
    for fp in processed_files:
        if fp in drive_paths:
            set_config(db, f'processed_file:{fp.name}', TODAY)

    # ── Dry run exit ──────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"\n  [DRY RUN] Processed {len(processed_files)} file(s). DB not saved.")
        return

    # ── Merge into DB ─────────────────────────────────────────────────────────
    print("\n  Merging into database...")

    if fk_pay_monthly or fk_ads_monthly:
        db['fk_monthly'] = merge_monthly(
            db.get('fk_monthly', []), fk_pay_monthly, 'fk', new_ads=fk_ads_monthly
        )

    if me_orders_monthly or me_sett_monthly or me_ads_monthly:
        db['me_monthly'] = merge_monthly(
            db.get('me_monthly', []), me_orders_monthly, 'me',
            new_sett=me_sett_monthly, new_ads=me_ads_monthly
        )

    if me_orders_skus or me_return_skus or me_catalog:
        db['me_skus'] = merge_me_skus(
            db.get('me_skus', []), me_orders_skus, me_return_skus, me_catalog
        )

    if fk_pay_skus or fk_views_skus:
        db['fk_skus'] = merge_fk_skus(
            db.get('fk_skus', []), fk_pay_skus, fk_views_skus
        )

    if me_return_reasons:
        db['me_return_reasons'] = build_return_reasons(
            db.get('me_return_reasons', []), me_return_reasons
        )

    if fk_keywords_data:
        db['fk_keywords'] = merge_fk_keywords(
            db.get('fk_keywords', []), fk_keywords_data
        )

    if fk_listings_pairs:
        db['fk_pairs'] = fk_listings_pairs  # replace each run — listing file is master data

    if me_claims_rows:
        db['me_claims'] = merge_claims(
            db.get('me_claims', []), me_claims_rows, 'ticket_id'
        )

    if fk_claims_rows:
        db['fk_claims'] = merge_claims(
            db.get('fk_claims', []), fk_claims_rows, 'order_id'
        )

    # ── Build daily tables ────────────────────────────────────────────────────
    print("\n  Building daily / keyword tables...")
    window_start = _daily_window_start()

    fk_daily_new = build_fk_daily(fk_views_paths, window_start) \
                   if fk_views_paths else []
    me_daily_new = build_me_daily(me_orders_paths, me_returns_paths, window_start) \
                   if (me_orders_paths or me_returns_paths) else []
    fk_kw_new    = build_fk_keywords(fk_keywords_paths) \
                   if fk_keywords_paths else []

    # Load existing daily + keywords and merge (new rows overwrite same date+sku)
    existing_daily = load_db(DB_DAILY_PATH)
    existing_kw    = load_db(DB_KEYWORDS_PATH)

    ex_fk = {(r['date'], r['sku_id']): r
             for r in existing_daily.get('fk_daily', [])}
    for r in fk_daily_new:
        ex_fk[(r['date'], r['sku_id'])] = r
    fk_daily_rows = [r for r in ex_fk.values()
                     if r.get('date', '') >= window_start]
    fk_daily_rows.sort(key=lambda r: (r['date'], r['sku_id']))

    ex_me = {(r['date'], r['sku_id']): r
             for r in existing_daily.get('me_daily', [])}
    for r in me_daily_new:
        ex_me[(r['date'], r['sku_id'])] = r
    me_daily_rows = [r for r in ex_me.values()
                     if r.get('date', '') >= window_start]
    me_daily_rows.sort(key=lambda r: (r['date'], r['sku_id']))

    # Keywords: merge on (month, sku_id, keyword) — full history, no window
    ex_kw = {(r.get('month', ''), r.get('sku_id', ''), r.get('keyword', '')): r
             for r in existing_kw.get('fk_keywords', [])}
    for r in fk_kw_new:
        ex_kw[(r.get('month', ''), r.get('sku_id', ''), r.get('keyword', ''))] = r
    kw_rows = sorted(ex_kw.values(),
                     key=lambda r: (r.get('month', ''), r.get('attributed_views', 0)),
                     reverse=True)

    # ── Update config ─────────────────────────────────────────────────────────
    set_config(db, 'last_updated', TODAY)
    all_daily_dates = ([r['date'] for r in fk_daily_rows if r.get('date')] +
                       [r['date'] for r in me_daily_rows  if r.get('date')])
    if all_daily_dates:
        set_config(db, 'daily_window_start', min(all_daily_dates))
        set_config(db, 'daily_window_end',   max(all_daily_dates))
    if kw_rows:
        set_config(db, 'keywords_last_updated', TODAY)

    # ── Save DB ───────────────────────────────────────────────────────────────
    save_db(db, DB_PATH)           # legacy rumee_db_v1.csv (kept during transition)
    save_db(db, DB_SUMMARY_PATH)   # rumee_db_summary.csv (dashboard overview)
    save_daily_csv(fk_daily_rows, me_daily_rows, DB_DAILY_PATH)
    save_keywords_csv(kw_rows, DB_KEYWORDS_PATH)

    # ── Update HTML date ──────────────────────────────────────────────────────
    if HTML_PATH.exists():
        update_html_date(HTML_PATH, TODAY)

    # ── Archive / cleanup ─────────────────────────────────────────────────────
    local_processed = [fp for fp in processed_files if fp not in drive_paths]
    drive_processed = [fp for fp in processed_files if fp in drive_paths]

    if local_processed:
        archive_dir = PROCESSED / TODAY
        archive_files(local_processed, archive_dir)

    if drive_processed:
        try:
            from drive_connector import cleanup_temp_files
            cleanup_temp_files(drive_processed)
            print(f"  Cleaned up {len(drive_processed)} Drive temp file(s)")
        except Exception as e:
            print(f"  Drive cleanup warning: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    summary_rows = sum(len(db.get(t, [])) for t in [
        'config', 'fk_monthly', 'me_monthly', 'fk_skus', 'me_skus',
        'me_return_reasons', 'fk_pairs', 'az_monthly', 'fk_keywords',
        'me_claims', 'fk_claims'])
    daily_rows   = len(fk_daily_rows) + len(me_daily_rows)
    daily_dates  = all_daily_dates  # already computed above
    daily_range  = (f"{min(daily_dates)} to {max(daily_dates)}"
                    if daily_dates else "no data")

    print(f"\n{'='*60}")
    print(f"  Done -- {TODAY}")
    print(f"  Files processed:  {len(processed_files)}")
    print(f"")
    print(f"  rumee_db_summary.csv: {summary_rows} rows")
    print(f"  rumee_db_daily.csv:   {daily_rows} rows ({daily_range})")
    print(f"  rumee_db_keywords.csv:{len(kw_rows)} rows")
    print(f"")
    print(f"  FK monthly: {len(db.get('fk_monthly', []))}  "
          f"ME monthly: {len(db.get('me_monthly', []))}")
    print(f"  FK SKUs:    {len(db.get('fk_skus', []))}  "
          f"ME SKUs:    {len(db.get('me_skus', []))}")
    print(f"  Return reasons: {len(db.get('me_return_reasons', []))}  "
          f"FK Keywords: {len(db.get('fk_keywords', []))}  "
          f"FK Pairs: {len(db.get('fk_pairs', []))}")
    print(f"\n  Next steps:")
    print(f"    git add rumee_db_v1.csv index.html")
    print(f"    git commit -m \"Data update: {TODAY}\"")
    print(f"    git push origin main")
    print(f"{'='*60}\n")

    # ── Generate Supabase insights + review completed tasks ──────────────────
    generate_insights(db)
    review_completed_tasks(db)

    # ── Email summary (only when --email flag is set) ──────────────────────────
    if args.email:
        _summary = {
            'date':                TODAY,
            'files_count':         len(processed_files),
            'files_detail':        '\n'.join(f'  - {fp.name}' for fp in processed_files),
            'summary_rows_added':  summary_rows,
            'daily_rows_added':    daily_rows,
            'keywords_rows_added': len(kw_rows),
            'daily_window_start':  min(all_daily_dates) if all_daily_dates else 'N/A',
            'daily_window_end':    max(all_daily_dates) if all_daily_dates else 'N/A',
            'fk_views_last_date':  fk_views_last,
            'me_orders_last_date': me_orders_last,
            'warnings':            '',
        }
        # Write summary to disk (useful for debugging CI runs)
        with open(BASE_DIR / 'processing_summary.json', 'w', encoding='utf-8') as _sf:
            json.dump(_summary, _sf, indent=2)
        send_summary_email(_summary)

# ─── Insights Generator ───────────────────────────────────────────────────────

def generate_insights(db):
    """
    Check latest SKU data against thresholds and write new insights to Supabase.
    Called after DB save on every successful pipeline run.
    Supabase failures are always swallowed — this never crashes the pipeline.
    """
    try:
        from supabase_connector import write_insight, write_task, insight_exists_today
    except ImportError:
        print("Supabase not configured — skipping insight generation")
        return

    if not os.environ.get('SUPABASE_URL'):
        print("SUPABASE_URL not set — skipping insight generation")
        return

    insights_written = 0

    # ── Meesho SKUs ──────────────────────────────────────────────────────────
    me_skus = [r for r in db.get('me_skus', []) if r.get('sku_id')]
    for sku in me_skus:
        sku_id       = sku.get('sku_id', '')
        sku_name     = sku.get('name', sku_id)
        return_rate  = float(sku.get('return_rate', 0))
        stock        = int(sku.get('stock', 999))
        total_orders = int(sku.get('total_orders', 0))
        sku_type     = sku.get('type', '')

        if sku_type == 'pause':
            continue

        # Return rate thresholds (only for active SKUs with >20 orders)
        if total_orders > 20:
            if return_rate > 20:
                if not insight_exists_today(sku_id, 'returns'):
                    insight = write_insight(
                        platform='meesho', sku_id=sku_id, sku_name=sku_name,
                        category='returns',
                        text=(f"{sku_name} return rate is {return_rate}% — above 20% critical threshold. "
                              f"Check packaging (missing chain), wrong SKU dispatched, listing photos."),
                        severity='critical'
                    )
                    if insight:
                        write_task(
                            task_text=(f"Investigate {sku_name} return rate ({return_rate}%). "
                                       f"Check: chain in packaging, correct SKU dispatched, listing photos match product."),
                            platform='meesho', sku_id=sku_id, priority='high',
                            linked_insight_id=insight['id']
                        )
                    insights_written += 1

            elif return_rate > 17:
                if not insight_exists_today(sku_id, 'returns'):
                    write_insight(
                        platform='meesho', sku_id=sku_id, sku_name=sku_name,
                        category='returns',
                        text=(f"{sku_name} return rate is {return_rate}% — "
                              f"approaching critical threshold (20%). Monitor closely."),
                        severity='warning'
                    )
                    insights_written += 1

        # Stock thresholds — Meesho
        if stock == 0:
            if not insight_exists_today(sku_id, 'stock'):
                insight = write_insight(
                    platform='meesho', sku_id=sku_id, sku_name=sku_name,
                    category='stock',
                    text=f"{sku_name} is OUT OF STOCK on Meesho. Restock urgently.",
                    severity='critical'
                )
                if insight:
                    write_task(
                        task_text=f"Restock {sku_name} on Meesho immediately — currently at zero.",
                        platform='meesho', sku_id=sku_id, priority='high',
                        linked_insight_id=insight['id']
                    )
                insights_written += 1

        elif 0 < stock < 50:
            if not insight_exists_today(sku_id, 'stock'):
                write_insight(
                    platform='meesho', sku_id=sku_id, sku_name=sku_name,
                    category='stock',
                    text=f"{sku_name} stock is low on Meesho ({stock} units remaining). Reorder soon.",
                    severity='warning'
                )
                insights_written += 1

    # ── Flipkart SKUs ─────────────────────────────────────────────────────────
    fk_skus = [r for r in db.get('fk_skus', []) if r.get('sku_id')]
    for sku in fk_skus:
        sku_id   = sku.get('sku_id', '')
        sku_name = sku.get('name', sku_id)
        stock    = int(sku.get('stock', 999))
        ctr      = float(sku.get('ctr', 0))
        revenue  = float(sku.get('ad_revenue', 0))
        sku_type = sku.get('type', '')

        if sku_type == 'pause':
            continue

        # Stock thresholds — Flipkart
        if stock == 0:
            if not insight_exists_today(sku_id, 'stock'):
                insight = write_insight(
                    platform='flipkart', sku_id=sku_id, sku_name=sku_name,
                    category='stock',
                    text=(f"{sku_name} is OUT OF STOCK on Flipkart. "
                          f"Remove from active campaigns immediately."),
                    severity='critical'
                )
                if insight:
                    write_task(
                        task_text=f"Remove {sku_name} from all active Flipkart campaigns. Restock urgently.",
                        platform='flipkart', sku_id=sku_id, priority='high',
                        linked_insight_id=insight['id']
                    )
                insights_written += 1

        elif 0 < stock < 50:
            if not insight_exists_today(sku_id, 'stock'):
                write_insight(
                    platform='flipkart', sku_id=sku_id, sku_name=sku_name,
                    category='stock',
                    text=(f"{sku_name} stock is low on Flipkart ({stock} units). "
                          f"Reorder before it hits zero."),
                    severity='warning'
                )
                insights_written += 1

        # High CTR with zero revenue → listing conversion problem
        if ctr > 3.0 and revenue == 0:
            if not insight_exists_today(sku_id, 'views'):
                insight = write_insight(
                    platform='flipkart', sku_id=sku_id, sku_name=sku_name,
                    category='views',
                    text=(f"{sku_name} has {ctr}% CTR but zero ad revenue — "
                          f"buyers clicking but not converting. Listing quality issue."),
                    severity='warning'
                )
                if insight:
                    write_task(
                        task_text=(f"Fix {sku_name} listing — {ctr}% CTR but zero conversions. "
                                   f"Check primary photo, price, and description match buyer expectation."),
                        platform='flipkart', sku_id=sku_id, priority='high',
                        linked_insight_id=insight['id']
                    )
                insights_written += 1

    # ── Meesho Claims ─────────────────────────────────────────────────────────
    me_claims = db.get('me_claims', [])
    if me_claims:
        from datetime import timedelta

        # 1. Reopen deadline approaching — open claims whose reopen_validity is within 5 days
        today_dt = date.today()
        deadline_soon = []
        for claim in me_claims:
            status = str(claim.get('status', '')).lower()
            if status in ('closed', 'resolved'):
                continue
            rv = str(claim.get('reopen_validity', '')).strip()
            if not rv or rv in ('nan', 'None', ''):
                continue
            try:
                rv_dt = datetime.strptime(rv[:10], '%Y-%m-%d').date()
                days_left = (rv_dt - today_dt).days
                if 0 <= days_left <= 5:
                    deadline_soon.append({
                        'ticket_id': claim.get('ticket_id', ''),
                        'order_id':  claim.get('order_id', ''),
                        'issue':     claim.get('issue_type', 'unknown'),
                        'days':      days_left,
                    })
            except (ValueError, TypeError):
                continue

        if deadline_soon:
            sku_id   = 'me-claims-deadline'
            category = 'claims'
            if not insight_exists_today(sku_id, category):
                details = '; '.join(
                    f"Ticket {c['ticket_id']} ({c['issue']}, {c['days']}d left)"
                    for c in deadline_soon[:5]
                )
                write_insight(
                    platform='meesho', sku_id=sku_id, sku_name='Claims',
                    category=category,
                    text=(f"{len(deadline_soon)} Meesho claim(s) have reopen deadline "
                          f"within 5 days. Act before losing recovery rights. {details}"),
                    severity='critical'
                )
                insights_written += 1

        # 2. Missed claims — closed tickets with zero recovery (potential unresolved loss)
        missed = [c for c in me_claims
                  if str(c.get('status', '')).lower() in ('closed', 'resolved')
                  and not c.get('amount_recovered')
                  and c.get('issue_type')]
        if len(missed) >= 3:
            sku_id   = 'me-claims-missed'
            category = 'claims'
            if not insight_exists_today(sku_id, category):
                write_insight(
                    platform='meesho', sku_id=sku_id, sku_name='Claims',
                    category=category,
                    text=(f"{len(missed)} Meesho tickets closed with zero payment recorded. "
                          f"Review closed claims to check if recoveries were missed."),
                    severity='warning'
                )
                insights_written += 1

    # ── Flipkart Claims ───────────────────────────────────────────────────────
    fk_claims = db.get('fk_claims', [])
    if fk_claims:
        # 3. Pending claim value — total approved_amount for non-resolved claims
        pending_claims = [c for c in fk_claims
                          if str(c.get('status', '')).lower()
                          not in ('paid', 'credited', 'closed', 'rejected')]
        pending_value  = 0.0
        for c in pending_claims:
            try:
                pending_value += float(c.get('approved_amount', 0) or 0)
            except (ValueError, TypeError):
                pass

        if pending_value >= 500:
            sku_id   = 'fk-claims-pending'
            category = 'claims'
            if not insight_exists_today(sku_id, category):
                write_insight(
                    platform='flipkart', sku_id=sku_id, sku_name='Claims',
                    category=category,
                    text=(f"₹{int(pending_value):,} in Flipkart claims are approved but "
                          f"not yet credited ({len(pending_claims)} claim(s)). "
                          f"Follow up with Flipkart seller support."),
                    severity='warning' if pending_value < 2000 else 'critical'
                )
                insights_written += 1

        # 4. High claims count — flag if we have many open/pending claims
        open_claims = [c for c in fk_claims
                       if str(c.get('status', '')).lower()
                       not in ('paid', 'credited', 'closed', 'rejected', 'resolved')]
        if len(open_claims) >= 10:
            sku_id   = 'fk-claims-volume'
            category = 'claims'
            if not insight_exists_today(sku_id, category):
                write_insight(
                    platform='flipkart', sku_id=sku_id, sku_name='Claims',
                    category=category,
                    text=(f"{len(open_claims)} Flipkart claims are open/pending. "
                          f"High volume may indicate a systemic fulfilment or returns issue."),
                    severity='warning'
                )
                insights_written += 1

    print(f"Insights generated: {insights_written} new insights written to Supabase")


# ─── Task Completion Review ───────────────────────────────────────────────────

def review_completed_tasks(db):
    """
    Check recently completed tasks against latest data.
    If the underlying issue persists despite being marked done — reopen it.
    If the issue is genuinely resolved — mark the linked insight as resolved.
    """
    try:
        try:
            from supabase_connector import (get_client, write_insight, write_task,
                                             mark_insight_resolved)
        except ImportError:
            return

        if not os.environ.get('SUPABASE_URL'):
            return

        client = get_client()

        # Fetch tasks marked done in the last 7 days that have a linked insight
        from datetime import timedelta, timezone as _tz
        cutoff = (datetime.now(_tz.utc) - timedelta(days=7)).isoformat()

        result = client.table('rumee_tasks')\
            .select('*, rumee_insights(*)')\
            .eq('status', 'done')\
            .gte('completed_at', cutoff)\
            .not_.is_('linked_insight_id', 'null')\
            .execute()

        completed_tasks = result.data or []
        if not completed_tasks:
            print("Task review: no recently completed tasks to check")
            return

        print(f"Reviewing {len(completed_tasks)} recently completed tasks...")

        # Build lookup maps from latest DB data
        me_sku_map = {r['sku_id']: r for r in db.get('me_skus', []) if r.get('sku_id')}
        fk_sku_map = {r['sku_id']: r for r in db.get('fk_skus', []) if r.get('sku_id')}

        reopened = 0
        resolved = 0

        for task in completed_tasks:
            insight = task.get('rumee_insights')
            if not insight:
                continue

            sku_id     = insight.get('sku_id')
            category   = insight.get('category')
            platform   = insight.get('platform')
            insight_id = insight.get('id')

            issue_persists = False
            current_value  = None

            # ── Check return rate (Meesho) ──
            if category == 'returns' and platform == 'meesho' and sku_id in me_sku_map:
                sku = me_sku_map[sku_id]
                return_rate = float(sku.get('return_rate', 0))
                current_value = f"{return_rate}% return rate"
                if return_rate > 17:   # warning threshold — if still above, issue persists
                    issue_persists = True

            # ── Check stock (Meesho) ──
            elif category == 'stock' and platform == 'meesho' and sku_id in me_sku_map:
                sku = me_sku_map[sku_id]
                stock = int(sku.get('stock', 999))
                current_value = f"{stock} units in stock"
                if stock < 50:
                    issue_persists = True

            # ── Check stock (Flipkart) ──
            elif category == 'stock' and platform == 'flipkart' and sku_id in fk_sku_map:
                sku = fk_sku_map[sku_id]
                stock = int(sku.get('stock', 999))
                current_value = f"{stock} units in stock"
                if stock < 50:
                    issue_persists = True

            # ── Check CTR with zero revenue (Flipkart) ──
            elif category == 'views' and platform == 'flipkart' and sku_id in fk_sku_map:
                sku = fk_sku_map[sku_id]
                ctr     = float(sku.get('ctr', 0))
                revenue = float(sku.get('ad_revenue', 0))
                current_value = f"CTR {ctr}%, revenue ₹{revenue}"
                if ctr > 3.0 and revenue == 0:
                    issue_persists = True

            # ── Act on result ──
            if issue_persists:
                # Mark original insight resolved (the task was completed, just didn't fix it)
                mark_insight_resolved(insight_id)

                # Write a new insight noting the task was done but issue persists
                sku_name = insight.get('sku_name', sku_id)
                completed_date = (task.get('completed_at', '') or '')[:10] or 'recently'

                new_insight = write_insight(
                    platform=platform,
                    sku_id=sku_id,
                    sku_name=sku_name,
                    category=category,
                    text=(f"{sku_name}: Task was marked done on {completed_date} but issue persists. "
                          f"Current status: {current_value}. Needs follow-up."),
                    severity='warning'
                )
                if new_insight:
                    write_task(
                        task_text=(f"Follow-up required on {sku_name} — task was marked done on {completed_date} "
                                   f"but {current_value}. Review what was done and try again."),
                        platform=platform,
                        sku_id=sku_id,
                        priority='high',
                        linked_insight_id=new_insight['id']
                    )
                reopened += 1

            else:
                # Issue is genuinely resolved — mark insight resolved
                if insight.get('status') != 'resolved':
                    mark_insight_resolved(insight_id)
                resolved += 1

        print(f"Task review: {resolved} issues confirmed resolved, {reopened} issues reopened")

    except Exception as e:
        print(f"Warning: review_completed_tasks failed: {e}")


# ─── Email Summary ────────────────────────────────────────────────────────────

def send_summary_email(summary):
    """
    Send a processing summary email via Gmail SMTP.
    Requires GMAIL_USER and GMAIL_APP_PASSWORD environment variables.
    Includes Supabase insights section when SUPABASE_URL is configured.
    Called only when --email flag is passed and processing succeeds.
    """
    gmail_user = os.environ.get('GMAIL_USER')
    gmail_pass = os.environ.get('GMAIL_APP_PASSWORD')

    if not gmail_user or not gmail_pass:
        print("Email skipped — GMAIL_USER or GMAIL_APP_PASSWORD not set")
        return

    # ── Fetch insights for email (3-day cap, dedup, stamps email_count) ───────
    insights_text = ''
    try:
        from supabase_connector import get_insights_for_email
        email_insights = get_insights_for_email()
        if email_insights:
            critical = [i for i in email_insights if i['severity'] == 'critical']
            warnings = [i for i in email_insights if i['severity'] == 'warning']
            info     = [i for i in email_insights if i['severity'] == 'info']
            if critical:
                insights_text += f"\n🔴 CRITICAL ({len(critical)}):\n"
                for i in critical:
                    cnt = i['email_count'] + 1
                    note = f" [notified {cnt}/3 days]" if i['email_count'] > 0 else ""
                    insights_text += f"  • {i.get('sku_name') or ''}: {i['insight_text']}{note}\n"
            if warnings:
                insights_text += f"\n⚠  WARNINGS ({len(warnings)}):\n"
                for i in warnings:
                    cnt = i['email_count'] + 1
                    note = f" [notified {cnt}/3 days]" if i['email_count'] > 0 else ""
                    insights_text += f"  • {i.get('sku_name') or ''}: {i['insight_text']}{note}\n"
            if info:
                insights_text += f"\nℹ  INFO ({len(info)}):\n"
                for i in info:
                    insights_text += f"  • {i.get('sku_name') or ''}: {i['insight_text']}\n"
        else:
            insights_text = "\nNo new insights — all metrics within thresholds.\n"
    except Exception:
        insights_text = "\nInsights unavailable (Supabase not configured or unreachable).\n"

    subject = f"Rumee Dashboard Updated — {summary['date']}"

    body = f"""
Rumee Dashboard — Data Update Summary
Date: {summary['date']}

FILES PROCESSED: {summary['files_count']}
{summary['files_detail']}

NEW ROWS ADDED:
  Summary DB: {summary['summary_rows_added']} rows
  Daily DB:   {summary['daily_rows_added']} rows
  Keywords:   {summary['keywords_rows_added']} rows

DATA COVERAGE:
  Daily window: {summary['daily_window_start']} → {summary['daily_window_end']}
  Last FK views: {summary['fk_views_last_date']}
  Last ME orders: {summary['me_orders_last_date']}

INSIGHTS:{insights_text}
Dashboard: https://rumeein.github.io/rumee-dashboard/
Repository: https://github.com/Rumeein/rumee-dashboard
"""

    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From']    = gmail_user
    msg['To']      = 'rumeein@gmail.com'
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_pass)
            server.send_message(msg)
        print(f"Summary email sent to rumeein@gmail.com")
    except Exception as e:
        print(f"Email failed: {e}")


if __name__ == '__main__':
    main()
