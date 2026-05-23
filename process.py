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

import os, sys, shutil, re, glob, csv
from datetime import date, datetime
from pathlib import Path
import pandas as pd

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
NEW_DATA   = BASE_DIR / "new_data"
PROCESSED  = BASE_DIR / "processed"
DB_PATH    = BASE_DIR / "rumee_db_v1.csv"
HTML_PATH  = BASE_DIR / "index.html"
TODAY      = date.today().isoformat()

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

def detect_file_type(path):
    """Return one of: ME_ORDERS, ME_RETURNS, ME_PAYMENTS, ME_ADS,
                      FK_PAYMENTS, FK_ADS, FK_VIEWS, FK_KEYWORDS,
                      CATALOG, UNKNOWN"""
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
        return 'UNKNOWN'
    elif ext in ('.xlsx', '.xls'):
        hdr = sniff_xlsx_header(path)
        if 'ads cost' in hdr and ('ad cost' in hdr or 'deduction' in hdr) and 'flipkart' not in hdr:
            return 'ME_ADS'
        if 'order related details' in hdr and 'sub order no' in hdr:
            return 'ME_PAYMENTS'
        if 'wallet redeem' in hdr or ('flipkart' in str(path).lower() and 'ads' in str(path).lower() and 'wallet' in hdr):
            return 'FK_ADS'
        if 'seller sku' in hdr and ('settlement' in hdr or 'bank settlement' in hdr or 'sale amount' in hdr):
            return 'FK_PAYMENTS'
        if 'system stock' in hdr or ('style id' in hdr and 'catalog' in hdr):
            return 'CATALOG'
        # fallback: try by filename
        name = path.stem.lower()
        if 'flipkart_ads' in name or 'ads_data' in name and 'flipkart' in name:
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
        s['return_rate']  = round((s['rto']) / total, 4) if total else 0
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

    # Date column
    date_col = next((c for c in df.columns if 'Return Created Date' in c or 'Dispatch Date' in c), None)
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

def process_meesho_payments(path, last_date_str):
    """Returns monthly: {month: settlement} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0], header=[0, 1])

    # Positional columns: 1=Order Date, 13=Final Settlement Amount
    order_date_col  = 1
    settlement_col  = 13

    dates = pd.to_datetime(df.iloc[:, order_date_col], errors='coerce').dt.date
    setts = pd.to_numeric(df.iloc[:, settlement_col], errors='coerce').fillna(0)

    valid = dates.notna()
    df2 = pd.DataFrame({'_dt': dates[valid], 'sett': setts[valid]})
    before = len(df2)
    df_new = df2[df2['_dt'] > last_date]
    new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  ME Payments: {len(df_new)} new rows, skipping {len(df2)-len(df_new)}")
    if len(df_new) == 0:
        return {}, str(new_last)

    monthly = {}
    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if mk:
            monthly[mk] = monthly.get(mk, 0) + float(row['sett'])

    return {k: round(v, 2) for k, v in monthly.items()}, str(new_last)

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

def process_fk_payments(path, last_date_str):
    """
    Returns:
        monthly: {month: {gmv, settlement, orders, returns}}
        skus:    {sku_id: {name, orders, returns, gmv, settlement}}
        new_last_date: str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    xl = pd.ExcelFile(path)
    df = xl.parse(xl.sheet_names[0], header=[0, 1])

    # Positional columns:
    # 3  = Bank Settlement Value
    # 9  = Sale Amount
    # 55 = Order Date
    # 58 = Seller SKU
    # 62 = Return Type

    dates     = pd.to_datetime(df.iloc[:, 55], errors='coerce').dt.date
    skus_raw  = df.iloc[:, 58].astype(str)
    sale_amt  = pd.to_numeric(df.iloc[:, 9], errors='coerce').fillna(0)
    sett_amt  = pd.to_numeric(df.iloc[:, 3], errors='coerce').fillna(0)
    ret_type  = df.iloc[:, 62].astype(str)

    valid = dates.notna()
    df2 = pd.DataFrame({
        '_dt': dates[valid], 'sku': skus_raw[valid], 'sale': sale_amt[valid],
        'sett': sett_amt[valid], 'ret': ret_type[valid]
    })
    df_new = df2[df2['_dt'] > last_date]
    new_last = df2['_dt'].max() if len(df2) else last_date

    print(f"  FK Payments: {len(df_new)} new rows ({df_new['_dt'].min() if len(df_new) else 'N/A'} to "
          f"{df_new['_dt'].max() if len(df_new) else 'N/A'}), skipping {len(df2)-len(df_new)}")
    if len(df_new) == 0:
        return {}, {}, str(new_last)

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

        m = monthly.setdefault(mk, {'gmv':0,'settlement':0,'orders':0,'returns':0})
        s = skus.setdefault(sid, {'name':sname,'type':'','orders':0,'returns':0,'gmv':0,'settlement':0})

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

    # Round
    for mk, m in monthly.items():
        m['gmv'] = round(m['gmv'], 2)
        m['settlement'] = round(m['settlement'], 2)
    for sid, s in skus.items():
        s['gmv'] = round(s['gmv'], 2)
        s['settlement'] = round(s['settlement'], 2)

    return monthly, skus, str(new_last)

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

# ─── Flipkart Views ───────────────────────────────────────────────────────────

def process_fk_views(path, last_date_str):
    """Returns skus: {sku_id: {views, clicks, sales, revenue, ctr}} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(path, dtype={'Impression Date': str})

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
            r['rto_rate']      = round(int(r.get('rto', 0)) / total, 4)
            r['cust_ret_rate'] = round(int(r.get('cust_returns', 0)) / total, 4)
            r['return_rate']   = round((int(r.get('rto', 0)) + int(r.get('cust_returns', 0))) / total, 4)
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

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Rumee Dashboard Pipeline — {TODAY}")
    print(f"{'='*60}")

    # Find files in new_data/
    NEW_DATA.mkdir(exist_ok=True)
    files = [f for f in NEW_DATA.iterdir() if f.is_file() and f.suffix.lower() in ('.csv', '.xlsx', '.xls')]
    if not files:
        print("\n  No files found in new_data/. Drop export files there and re-run.")
        return

    print(f"\n  Found {len(files)} file(s) in new_data/:")
    typed = {}
    for f in files:
        ft = detect_file_type(f)
        typed[f] = ft
        print(f"    {f.name:50s} = {ft}")

    # Load existing DB
    db = load_db(DB_PATH)
    if not db:
        print("\n  No existing DB — will create fresh database.")
        db = {'config': [], 'fk_monthly': [], 'me_monthly': [], 'fk_skus': [],
              'me_skus': [], 'me_return_reasons': [], 'fk_pairs': [], 'az_monthly': []}

    # Get last processed dates
    me_orders_last   = get_config(db, 'me_orders_last_date')
    me_returns_last  = get_config(db, 'me_returns_last_date')
    me_payments_last = get_config(db, 'me_payments_last_date')
    me_ads_last      = get_config(db, 'me_ads_last_date')
    fk_payments_last = get_config(db, 'fk_payments_last_date')
    fk_ads_last      = get_config(db, 'fk_ads_last_date')
    fk_views_last    = get_config(db, 'fk_views_last_date')

    processed_files = []
    summary = []

    # ── Process each file ────────────────────────────────────────────────────
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

    for fp, ft in typed.items():
        print(f"\n  Processing: {fp.name} ({ft})")

        if ft == 'ME_ORDERS':
            m, s, new_last = process_meesho_orders(fp, me_orders_last)
            me_orders_monthly.update(m)
            for sid, nd in s.items():
                if sid in me_orders_skus:
                    me_orders_skus[sid]['delivered'] += nd['delivered']
                    me_orders_skus[sid]['rto'] += nd['rto']
                    me_orders_skus[sid]['gmv'] += nd['gmv']
                else:
                    me_orders_skus[sid] = nd
            if new_last > me_orders_last:
                me_orders_last = new_last
                set_config(db, 'me_orders_last_date', new_last)
            summary.append(f"Meesho Orders: {sum(len(v) for v in [m.items()])} months updated")
            processed_files.append(fp)

        elif ft == 'ME_RETURNS':
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
            m, new_last = process_meesho_payments(fp, me_payments_last)
            for mk, sett in m.items():
                me_sett_monthly[mk] = me_sett_monthly.get(mk, 0) + sett
            if new_last > me_payments_last:
                me_payments_last = new_last
                set_config(db, 'me_payments_last_date', new_last)
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
            m, s, new_last = process_fk_payments(fp, fk_payments_last)
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
            if new_last > fk_payments_last:
                fk_payments_last = new_last
                set_config(db, 'fk_payments_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_ADS':
            m, new_last = process_fk_ads(fp, fk_ads_last)
            for mk, ads in m.items():
                fk_ads_monthly[mk] = fk_ads_monthly.get(mk, 0) + ads
            if new_last > fk_ads_last:
                fk_ads_last = new_last
                set_config(db, 'fk_ads_last_date', new_last)
            processed_files.append(fp)

        elif ft == 'FK_VIEWS':
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
            print("  FK Keywords: indexed for future use (not yet stored in DB)")
            processed_files.append(fp)

        elif ft == 'CATALOG':
            me_catalog = process_catalog(fp)
            processed_files.append(fp)

        else:
            print(f"  UNKNOWN file type — skipping {fp.name}")

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

    # ── Update config ─────────────────────────────────────────────────────────
    set_config(db, 'last_updated', TODAY)

    # ── Save DB ───────────────────────────────────────────────────────────────
    save_db(db, DB_PATH)

    # ── Update HTML date ──────────────────────────────────────────────────────
    if HTML_PATH.exists():
        update_html_date(HTML_PATH, TODAY)

    # ── Archive processed files ───────────────────────────────────────────────
    if processed_files:
        archive_dir = PROCESSED / TODAY
        archive_files(processed_files, archive_dir)

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Done — {TODAY}")
    print(f"  Files processed:  {len(processed_files)}")
    print(f"  FK monthly rows:  {len(db.get('fk_monthly', []))}")
    print(f"  ME monthly rows:  {len(db.get('me_monthly', []))}")
    print(f"  FK SKUs:          {len(db.get('fk_skus', []))}")
    print(f"  ME SKUs:          {len(db.get('me_skus', []))}")
    print(f"  Return reasons:   {len(db.get('me_return_reasons', []))}")
    print(f"\n  Next steps:")
    print(f"    git add rumee_db_v1.csv index.html")
    print(f"    git commit -m 'Data update: {TODAY}'")
    print(f"    git push origin main")
    print(f"{'='*60}\n")

if __name__ == '__main__':
    main()
