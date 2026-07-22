"""
Rumee Dashboard Data Pipeline
Processes raw export files from Meesho and Flipkart seller panels,
writes DB CSVs to rumee-data private repo (via RUMEE_DATA_DIR), and
writes summary data to Firestore for the dashboard to read.

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

import os, sys, shutil, re, glob, csv, argparse, json, io
from datetime import date, datetime
from pathlib import Path
import pandas as pd

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).parent
NEW_DATA  = BASE_DIR / "new_data"
PROCESSED = BASE_DIR / "processed"

# RUMEE_DATA_DIR: set by GitHub Actions to the cloned rumee-data private repo.
# Falls back to BASE_DIR for local development.
DATA_DIR         = Path(os.environ.get('RUMEE_DATA_DIR', BASE_DIR))
DB_SUMMARY_PATH  = DATA_DIR / "rumee_db_summary.csv"
DB_DAILY_PATH    = DATA_DIR / "rumee_db_daily.csv"
DB_KEYWORDS_PATH = DATA_DIR / "rumee_db_keywords.csv"
DB_FK_ADS_PATH   = DATA_DIR / "rumee_db_fk_ads.csv"
DB_ME_ADS_PATH   = DATA_DIR / "rumee_db_me_ads.csv"
DB_ALLTIME_PATH  = DATA_DIR / "rumee_db_alltime.csv"

# Maps each pipeline stream (matches data_pipeline_manifest.json `id`) to the
# distinctive DB tables it populates. Used to compute LIVE pipeline status each
# run (see run-log build): all tables have rows -> ok, some -> partial, none ->
# gap. Streams with no DB table (me_catalog) or no source (az_all) are left out
# so the dashboard falls back to the manifest's static status for them.
_STREAM_TABLES = {
    'me_orders':   ['me_monthly', 'me_skus'],
    'me_returns':  ['me_return_reasons'],
    'me_payments': ['me_monthly'],
    'me_ads':      ['me_ads_daily', 'me_ads_catalog', 'me_ads_master'],
    'me_views':    ['me_views'],
    'me_claims':   ['me_claims'],
    'fk_payments': ['fk_monthly', 'fk_skus'],
    'fk_orders':   ['fk_orders_daily', 'fk_orders_sku'],
    'fk_returns':  ['fk_returns_daily', 'fk_return_reasons'],
    'fk_views':    ['fk_daily'],
    'fk_keywords': ['fk_keywords'],
    'fk_ads':      ['fk_ads_daily', 'fk_ads_sku', 'fk_ads_kw', 'fk_ads_placements',
                    'fk_ads_overall', 'fk_ads_search', 'fk_ads_order_items'],
    'fk_claims':   ['fk_claims'],
    'fk_listings': ['fk_pairs'],
    # Amazon (dashboard memory active.md #57, 2026-07-14) — replaces the old
    # az_all placeholder (no extension fed Amazon at all) now that the SP-API
    # Reports acquisition exists. az_monthly is now a derived rollup of
    # az_orders_daily (_az_monthly_rollup), not its own live API call --
    # process_az_monthly retired 2026-07-15 once Orders/Settlement/Returns
    # were confirmed working against real data.
    'az_monthly':    ['az_monthly'],
    'az_orders':     ['az_orders_daily'],
    'az_settlement': ['az_settlement'],
    'az_returns':    ['az_returns_daily'],
}

# ─── Auto-Sync manifest cross-check ──────────────────────────────────────────
# Maps download_manifest.csv's stable "File Name" slot labels (see rumee-
# auto-sync DOCS.md Section 25) to our stream ids. meesho_ads_master.csv and
# every meesho_ads_<campaign>_{summary,catalog}_<date>.csv file (plus the
# literal 'meesho_ads_*_summary'/'meesho_ads_*_catalog' placeholder rows
# Auto-Sync writes when no campaign is active that day) all roll up to me_ads
# via a startswith('meesho_ads_') fallback in _build_manifest_cross_check —
# not listed individually here since the campaign id/date varies per file.
_MANIFEST_SLOT_TO_STREAM = {
    'meesho_orders':    'me_orders',
    'meesho_returns':   'me_returns',
    'meesho_payments':  'me_payments',
    'meesho_tickets':   'me_claims',
    'meesho_inventory': 'me_catalog',
    'meesho_views.csv': 'me_views',
    'flipkart_orders':          'fk_orders',
    'flipkart_returns':         'fk_returns',
    'flipkart_payments':        'fk_payments',
    'flipkart_ads_daily':       'fk_ads',
    'flipkart_ads_fsn':         'fk_ads',
    'flipkart_ads_placements':  'fk_ads',
    'flipkart_ads_overall':     'fk_ads',
    'flipkart_ads_search_terms':'fk_ads',
    'flipkart_ads_orders':      'fk_ads',
    'flipkart_ads_keywords':    'fk_ads',
    'flipkart_views':    'fk_views',
    'flipkart_claims':   'fk_claims',
    'flipkart_listings': 'fk_listings',
    'flipkart_keywords': 'fk_keywords',
}

MANIFEST_CROSS_CHECK_WINDOW_DAYS = 14

# processed_file:{prefix}_{original filename}, set by drive_connector /
# fetch_new_files the moment a file is downloaded — the original filename
# (Auto-Sync's own naming convention) embeds the Data Date, so this is a
# real per-date "did the pipeline receive and attempt this exact file"
# signal, independent of whether the file's CONTENT had any rows worth
# writing to a DB table. That distinction matters: a first version of this
# cross-check used each stream's own DB-table date range instead, and it
# produced dozens of false "gap" discrepancies for two different reasons,
# both confirmed against real production data 2026-07-11:
#   1. Watermark-only streams (claims, keywords, listings, catalog, payments)
#      only advance their `*_last_date` watermark when a file contains a row
#      NEWER than the current cutoff — a file that legitimately has nothing
#      new (e.g. fk_claims genuinely had zero new claims for weeks, a known,
#      already-confirmed fact) still gets downloaded and processed every day,
#      but the watermark-based check couldn't tell "file arrived, nothing new
#      inside" apart from "file never arrived".
#   2. Per-day report files (fk_ads, fk_returns) can legitimately contain
#      ZERO data rows for a given day (no ad spend, no returns that day) —
#      confirmed by downloading the real flipkart_ads_daily files for several
#      flagged dates and finding 3-line files (header only, no campaign
#      rows). The DB table correctly has no rows for that date, but that's
#      not a gap — it's an accurate reflection of a quiet day.
# processed_file keys sidestep both: they answer "did the pipeline touch
# this file" without caring how many rows it contained.
#
# The one exception: me_views is an APPEND-type source (a single rolling
# meesho_views.csv, deduped by Drive modifiedTime via `processed_modified:`,
# not a per-day `processed_file:` key) — for that stream, its own per-row
# Date column (already read into the `me_views` table) is the only real
# per-day signal, and is used directly instead.
_STREAM_FILE_PREFIXES = {
    'me_orders':   ('me_orders',),
    'me_returns':  ('me_returns',),
    'me_payments': ('me_payments',),
    'me_ads':      ('me_ads_master', 'me_ads_summary', 'me_ads_catalog'),
    'fk_payments': ('fk_payments',),
    'me_claims':   ('me_claims',),
    'me_catalog':  ('catalog',),
    'fk_orders':   ('fk_orders',),
    'fk_returns':  ('fk_returns',),
    'fk_views':    ('fk_views',),
    'fk_keywords': ('fk_keywords',),
    'fk_ads':      ('fk_ads_daily', 'fk_ads_fsn', 'fk_ads_placements',
                    'fk_ads_overall', 'fk_ads_search', 'fk_ads_orders', 'fk_ads_kw'),
    'fk_claims':   ('fk_claims',),
    'fk_listings': ('fk_listings',),
    # me_views intentionally omitted — see note above, uses its own table instead.
}

def _dated_processed_files(db, *prefixes):
    """
    Set of 'YYYY-MM-DD' dates extracted from processed_file:{prefix}_... config
    keys for any of the given prefixes — see _STREAM_FILE_PREFIXES for why this
    is the cross-check's per-stream date signal instead of DB-table row dates.

    Extracts EVERY date embedded in the filename, not just the first — some
    exports arrive as a range file covering more than one day (e.g. an FK
    Claims file confirmed 2026-07-11: 'flipkart_claims_2026-07-05_2026-07-06
    .xlsx' covers both dates in one file). A first version of this used
    re.search (first match only), which silently dropped the file's second
    date from the set even though the pipeline genuinely processed it.
    """
    dates = set()
    for r in db.get('config', []):
        key = r.get('key', '')
        if not key.startswith('processed_file:'):
            continue
        for p in prefixes:
            if key.startswith(f'processed_file:{p}_'):
                dates.update(re.findall(r'20\d{2}-\d{2}-\d{2}', key))
                break
    return dates

def _build_manifest_cross_check(manifest_rows, daily_dates_by_stream, today_str):
    """
    Cross-checks Auto-Sync's download_manifest (a Google Sheet, formerly a
    CSV — see drive_connector.fetch_download_manifest) — what Auto-Sync
    claims it produced, per file per day — against what THIS pipeline
    actually ingested — answers "is the data we generate actually landing
    and being used," not just "did Auto-Sync think it uploaded something."

    Two discrepancy types:
      - manifest_verified_pipeline_missing: Auto-Sync says the file landed,
        but the pipeline never received/processed it for that date — upload
        may have been wrong/corrupted, or the file was rejected in transit.
      - manifest_missing_pipeline_has_data: Auto-Sync says it never landed,
        but the pipeline processed something for that date anyway — usually
        a stale manifest row (see Section 25 history) or a manual/backfill
        upload the manifest never saw.

    Only the most recent MANIFEST_CROSS_CHECK_WINDOW_DAYS data-dates present
    in the manifest are checked — older rows are exactly the ones Section 25
    flags as unreliable for slots verified by rolling-file content (a Missing
    row can later flip to Verified in place), so re-litigating them would add
    noise, not signal.

    Args:
        manifest_rows: list of {'run_date','data_date','file_name','status'}
            from drive_connector.fetch_download_manifest().
        daily_dates_by_stream: {stream_id: set(of 'YYYY-MM-DD' dates)} — see
            _dated_processed_files (all streams) / me_views' own table (the
            one exception, an append-type source with no per-day filename).
        today_str: TODAY, stamped onto the result.

    Returns:
        None if manifest_rows is empty (Drive fetch failed this run) — caller
        must render "cross-check unavailable", never "0 discrepancies found".
    """
    if not manifest_rows:
        return None

    _by_key = {}  # (stream_id, data_date) -> {'verified': n, 'missing': n}
    for r in manifest_rows:
        fname = r.get('file_name', '')
        stream_id = _MANIFEST_SLOT_TO_STREAM.get(fname)
        if not stream_id and fname.startswith('meesho_ads_'):
            stream_id = 'me_ads'
        ddate = r.get('data_date', '')
        if not stream_id or not ddate:
            continue
        agg = _by_key.setdefault((stream_id, ddate), {'verified': 0, 'missing': 0})
        if r.get('status') == 'Verified':
            agg['verified'] += 1
        elif r.get('status') == 'Missing':
            agg['missing'] += 1

    _window = set(sorted({d for (_s, d) in _by_key}, reverse=True)[:MANIFEST_CROSS_CHECK_WINDOW_DAYS])

    streams_out = {}
    total = 0
    for (stream_id, ddate), agg in _by_key.items():
        if ddate not in _window:
            continue
        manifest_verified = agg['verified'] > 0
        pipeline_has_data = ddate in daily_dates_by_stream.get(stream_id, set())

        entry = streams_out.setdefault(stream_id, {'checked_dates': 0, 'discrepancies': []})
        entry['checked_dates'] += 1

        if manifest_verified and not pipeline_has_data:
            entry['discrepancies'].append({'date': ddate, 'manifest': 'Verified', 'pipeline': 'missing'})
            total += 1
        elif not manifest_verified and pipeline_has_data:
            entry['discrepancies'].append({'date': ddate, 'manifest': 'Missing', 'pipeline': 'has_data'})
            total += 1

    for entry in streams_out.values():
        entry['discrepancies'].sort(key=lambda d: d['date'], reverse=True)

    return {
        'checked_at': today_str,
        'window_days': MANIFEST_CROSS_CHECK_WINDOW_DAYS,
        'streams': streams_out,
        'total_discrepancies': total,
    }

HTML_PATH = BASE_DIR / "index.html"
TODAY     = date.today().isoformat()
LOG_PATH  = BASE_DIR / "pipeline_log.txt"

# ─── Run-wide error/warning tracker (Notification Center, active.md #70,
#     2026-07-20) ────────────────────────────────────────────────────────────
# Module-level (not local to main()) so the standalone per-file parser
# functions defined below (process_meesho_payments, process_fk_listings,
# process_az_catalog, etc.) can append directly on a caught exception
# without threading a new parameter through every signature and call site --
# main() re-points these to fresh lists at the start of every run (see
# `global _run_errors, _run_warnings` there) so nothing leaks across runs.
# Each entry: {'file','type','reason','impact'} -- 'impact' is what actually
# reaches the Notification Center in plain language; entries that predate
# this field (or a call site that didn't bother writing one) fall back to a
# generic type-derived impact string in sync_pipeline_notifications() rather
# than showing nothing.
_run_errors   = []
_run_warnings = []

# ─── Safe numeric helpers ────────────────────────────────────────────────────
# CSV rows may contain empty strings for numeric fields. float('') and int('')
# both raise ValueError. Use these helpers everywhere instead of bare float()/int().
def _flt(v, default=0.0):
    """Convert v to float safely. Returns default for None, '', 'N/A', or unparseable."""
    try:
        return float(v) if v not in (None, '', 'N/A', 'n/a', '-') else default
    except (TypeError, ValueError):
        return default

def _int(v, default=0):
    """Convert v to int safely via float to handle '1.0' style strings."""
    try:
        return int(float(v)) if v not in (None, '', 'N/A', 'n/a', '-') else default
    except (TypeError, ValueError):
        return default

# ─── Date comparison helper ───────────────────────────────────────────────────
# pandas 2.x stores date objects as datetime64[ns] in DataFrames, so comparing
# a _dt column (datetime64) against a Python date object raises TypeError.
# Use this helper everywhere instead of plain `df['_dt'] > last_date`.
def _dt_gt(series, last_date):
    """series > last_date, works for both datetime64[ns] and object dtype."""
    return pd.to_datetime(series, errors='coerce') > pd.Timestamp(last_date)

def _dt_le(series, last_date):
    """series <= last_date, works for both datetime64[ns] and object dtype."""
    return pd.to_datetime(series, errors='coerce') <= pd.Timestamp(last_date)

# Catalog/seller names whose rows must never be written to product_master.
# Lower-cased; matched as substring of PRODUCT NAME (and CATALOG NAME) column.
# 'meera craft' (not 'meera craft store') — 52 rows say only "Meera Craft"
# without the word "Store"; the stricter string missed them (170 total rows,
# confirmed 2026-07-03).
ME_CATALOG_BLOCKLIST = {'meera craft'}

# Permanent catalog-ID blocklist — these 45 Meera Craft catalog IDs are known
# junk and will never be re-listed. Blocked by exact CATALOG ID match so they
# stay blocked regardless of any future text/spelling change in the report.
# Belt-and-suspenders alongside the text match above (confirmed 2026-07-03).
ME_CATALOG_ID_BLOCKLIST = {
    '87120322', '87120687', '87122728', '87127860', '87128064', '87128789',
    '87133051', '87133265', '87135188', '87135189', '87135786', '87135787',
    '87135788', '87135974', '87135975', '87135976', '87136009', '87136017',
    '87136853', '87136880', '87137398', '87137402', '87137415', '87137631',
    '87137672', '87137813', '87137860', '87138400', '87138401', '87138402',
    '87138987', '87138988', '89106026', '89106054', '89106069', '89106074',
    '89106083', '89106397', '89106406', '89106409', '89106420', '89106422',
    '89106423', '89106502', '89106505',
}

# ─── Tenant Config ───────────────────────────────────────────────────────────────────────────
_TENANT_CFG_PATH = BASE_DIR / "tenant_config.json"
with open(_TENANT_CFG_PATH, encoding="utf-8") as _f:
    _TENANT_CFG = __import__("json").load(_f)

TENANT_ID = _TENANT_CFG["tenant_id"]

# ─── SKU Mappings (loaded from tenant_config.json) ─────────────────────────────
# Only the sales/ads rollup maps remain — me_sku_id()/fk_sku_id() use these.
# product_master is label-only via pm_overrides (Option A), so the old
# product_master helpers (design_map, base_variation_skus, az_sku_map, the
# *_for_pm resolvers) are no longer read here.
ME_SKU_MAP        = {k: tuple(v) for k, v in _TENANT_CFG["me_sku_map"].items()}
FK_SKU_MAP        = {k: tuple(v) for k, v in _TENANT_CFG["fk_sku_map"].items()}

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
    """Map raw Meesho SKU to (sku_id, display_name).
    Used for orders/ads/returns/payments aggregation — unchanged behavior,
    still auto-slugifies unmapped SKUs (these are report rollups, not
    product_master docs, so a slug key here is harmless and pre-existing).
    product_master no longer uses any slug map — it is label-only via
    pm_overrides (Option A)."""
    raw = str(raw_sku).strip()
    if raw in ME_SKU_MAP:
        return ME_SKU_MAP[raw]
    slug = re.sub(r'[^a-z0-9]', '-', raw.lower()).strip('-')
    return (f"me-{slug}", raw)

def fk_sku_id(raw_sku):
    """Map raw FK SKU to (sku_id, display_name).
    Used for orders/ads/returns/payments aggregation — unchanged behavior,
    still auto-slugifies unmapped SKUs (these are report rollups, not
    product_master docs, so a slug key here is harmless and pre-existing).
    product_master no longer uses any slug map — it is label-only via
    pm_overrides (Option A)."""
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
        'fk_monthly':       ['month', 'label', 'gmv', 'settlement', 'orders', 'returns', 'ad_spend',
                             'shopsy_orders', 'shopsy_revenue', 'reverse_shipping_cost'],
        'me_monthly':       ['month', 'label', 'gmv', 'settlement', 'orders', 'returns', 'ad_spend'],
        'fk_skus':          ['sku_id', 'name', 'type', 'mrp', 'selling', 'settlement', 'stock',
                             'ctr', 'ad_revenue', 'ad_spend', 'roas', 'conversions', 'ad_views', 'reverse_shipping_fee',
                             'return_rate', 'rto_rate', 'net_pl', 'commission'],
        'me_state_summary': ['state', 'orders', 'delivered', 'rto', 'rto_rate_pct', 'gmv', 'top_skus'],
        'fk_zone_summary':  ['zone', 'orders', 'revenue', 'returns', 'return_rate_pct'],
        'me_skus':          ['sku_id', 'name', 'type', 'total_orders', 'delivered', 'rto',
                             'cust_returns', 'return_rate', 'cust_ret_rate', 'rto_rate',
                             'gmv', 'avg_price', 'incomplete', 'wrong_product', 'quality'],
        'me_return_reasons':['reason', 'count', 'pct'],
        'fk_return_reasons':['reason', 'count', 'pct'],
        'fk_pairs':         ['base', 'og_name', 'og_mrp', 'og_selling', 'og_settlement',
                             'og_url', 'bahu_name', 'bahu_mrp', 'bahu_selling', 'bahu_settlement',
                             'bahu_url', 'status', 'verdict'],
        'az_monthly':       ['month', 'label', 'gmv', 'orders', 'returns', 'settlement', 'ad_spend'],
        'fk_keywords':      ['keyword', 'views', 'clicks', 'orders', 'revenue',
                             'ctr', 'conversion_rate'],
        'me_claims':        ['order_id', 'suborder_id', 'ticket_id', 'status', 'issue_type',
                             'created_date', 'last_update', 'reopen_validity',
                             'amount_recovered', 'transaction_id'],
        'fk_claims':        ['claim_id', 'incident_id', 'order_id', 'order_item_id', 'source',
                             'created_at', 'updated_at', 'status', 'approved_amount',
                             'not_approved_reason', 'auto_claim_reason'],
        'me_views':         ['date', 'views', 'orders'],
    }
    table_order = [
        'config', 'fk_monthly', 'me_monthly', 'fk_skus', 'me_skus',
        'me_return_reasons', 'fk_return_reasons', 'fk_pairs', 'az_monthly', 'fk_keywords',
        'me_claims', 'fk_claims', 'me_views', 'me_state_summary', 'fk_zone_summary',
    ]
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
                 'top_return_reason', 'states', 'total_units', 'shippable_units', 'ad_orders'],
    'fk_orders_daily': ['date', 'orders', 'quantity'],
    'fk_orders_sku':   ['date', 'sku', 'orders', 'quantity'],
    'fk_returns_daily': ['date', 'returns', 'courier_returns', 'customer_returns', 'quantity'],
    'fk_returns_sku':   ['date', 'sku', 'returns', 'courier_returns', 'customer_returns', 'quantity'],
    # Amazon (dashboard memory active.md #57) — keyed by order_id, not date,
    # since these persist per-order across runs so settlement/returns data
    # that arrives weeks after the order can still find and update it.
    # status (active.md item #66, 2026-07-18): canonical order-outcome
    # status -- 'placed' (real order, no confirmed final outcome yet,
    # sourced from the Orders report -- Amazon's Orders API has no
    # DELIVERED concept at all, confirmed against official SP-API docs) or
    # 'return' (a Refund transaction-type line appeared against this order
    # in a Settlement report -- Amazon doesn't distinguish RTO vs customer
    # return at the settlement level, only "was this order refunded").
    # Settlement always overrides Orders once it has an opinion -- see
    # _az_apply_settlement_status.
    'az_orders_daily':   ['order_id', 'order_date', 'platform', 'sku', 'qty', 'gmv', 'zone', 'is_shopsy', 'status'],
    'az_returns_daily':  ['order_id', 'return_date', 'return_reason', 'tracking_id', 'sku'],
    'az_settlement':     ['order_id', 'settlement', 'commission', 'shipping_fwd', 'tcs', 'fixed_fee'],
    # Idempotency ledger for return stock credit-back (active.md item #64,
    # 2026-07-17) -- fetch_return_receipts() always returns the FULL current
    # sheet, not just new rows, so this table is what stops the same return
    # from crediting stock back twice. One row per order_id; each condition
    # column tracks whether THAT specific component has already been
    # credited (Return Receipts scores earring/box/chain intact-ness
    # separately, and credit-back matches that granularity per Jaiswal).
    'stock_return_credits': ['order_id', 'earring_credited', 'box_credited', 'chain_credited', 'credited_at'],
    # Unbounded order_id -> sku history for FK/Meesho (active.md item #64,
    # 2026-07-17) -- neither platform's DAILY tables persist per-order SKU
    # (only day+sku aggregates), so without this, a return scanned weeks
    # after its order's own pipeline run has zero way to find out what SKU
    # was actually ordered. Amazon doesn't need this -- az_orders_daily
    # already persists order_id->sku for its whole history. Same "no
    # window_start cutoff" reasoning as az_orders_daily: a return can arrive
    # long after the order, so this can never be safely windowed/dropped.
    # status (active.md item #66, 2026-07-18): canonical order-outcome
    # status -- 'placed' (real order, no confirmed final outcome yet --
    # from the Orders file, which is only reliable for "was this ever a
    # real order" via cancelled-exclusion, NOT for delivered/RTO/return,
    # confirmed unreliable against a real live sample), 'cancelled'
    # (Orders file said CANCELLED/LOST -- reliable, an order cancelled
    # before shipment never reaches the Payments file at all, confirmed
    # against a real live sample), or 'delivered'/'rto'/'return' (from the
    # Payments file's own status column -- ALWAYS overrides whatever the
    # Orders file guessed, confirmed via real live samples for both
    # platforms that a row only appears in Payments once an order reaches
    # one of these 3 final outcomes). See _me_apply_payment_status /
    # _fk_apply_payment_status.
    # Extended (active.md item #67, 2026-07-19) beyond order_id/sku/date/
    # status to carry the full set of Ledger-relevant financial fields --
    # this registry is now ALSO the source of Orders Ledger rows, not just
    # the status filter/stock-decrement input it started as. status stays
    # the LOWERCASE canonical vocabulary used by the dashboard filter
    # ('placed'/'delivered'/'rto'/'return'/'cancelled') -- the Ledger's own
    # TitleCase labels ('In-Transit'/'Delivered'/...) are a translation
    # applied only when building ledger rows, never stored here.
    'fk_order_sku_index': ['order_id', 'sku', 'order_date', 'status', 'qty', 'gmv',
                            'settlement', 'commission', 'fixed_fee', 'collection_fee',
                            'shipping_fwd', 'shipping_rev', 'gst_on_fees', 'tcs', 'tds',
                            'penalty', 'zone', 'is_shopsy'],
    'me_order_sku_index': ['order_id', 'sku', 'sku_name', 'order_date', 'status', 'qty',
                            'gmv', 'settlement', 'commission', 'fixed_fee', 'collection_fee',
                            'shipping_fwd', 'shipping_rev', 'gst_on_fees', 'tcs', 'tds',
                            'penalty', 'zone', 'is_shopsy'],
    # Compact per-day-per-status order counts, derived from the two indices
    # above via _status_daily_rollup (active.md item #67, 2026-07-18) --
    # powers the dashboard's status filter without pushing the unbounded
    # raw per-order index to Firestore.
    'fk_orders_status_daily': ['date', 'status', 'orders'],
    'me_orders_status_daily': ['date', 'status', 'orders'],
    # Persisted order_id -> AWB, for the same reason as the sku indices
    # above: Return Receipts can be scanned with only the AWB captured (no
    # order_id/suborder number), and the existing Ledger builders already
    # resolve that via receipts.get(oid) or receipts.get(awb_index.get(oid))
    # -- but their awb_index dicts (fk_order_awb_index/me_suborder_awb_index)
    # are only built from THIS run's freshly-processed FK_RETURNS/ME_RETURNS
    # files and discarded after the run. A return can be scanned in a later
    # pipeline run than the one that first saw its AWB, so without
    # persisting this too, the same "resolved late" gap that motivated the
    # sku indices would still exist one level down. Sourced from the same
    # fk_order_awb_index/me_suborder_awb_index dicts already built during
    # returns processing (main(), ~line 6234) -- just persisted instead of
    # discarded.
    'fk_order_awb_index': ['order_id', 'awb'],
    'me_order_awb_index': ['order_id', 'awb'],
    # Persisted order_id -> sku, straight from each platform's own RETURNS
    # report (FK_RETURNS/ME_RETURNS "SKU" column) -- not to be confused with
    # fk_order_sku_index/me_order_sku_index above, which come from ORDERS
    # data. Added 2026-07-21 (dashboard memory active.md item #72, Jaiswal's
    # explicit ask): once a return has actually synced, its OWN reported SKU
    # is preferred over the order-placement-time guess for the Returns
    # Scanner's live lookup -- same unbounded/never-windowed reasoning as
    # fk_order_awb_index above (a return can be scanned in a later pipeline
    # run than the one that first saw it).
    'fk_return_sku_index': ['order_id', 'sku'],
    'me_return_sku_index': ['order_id', 'sku'],
    # Amazon Search Query Performance (Brand Analytics) -- one row per
    # (asin, search_query, period_start), full history retained (keyed by
    # those 3 fields on merge, not windowed) same reasoning as the 3 tables
    # above: this is the sole source for keyword-level Amazon reporting.
    'az_search_terms':   ['period_type', 'period_start', 'period_end', 'asin', 'search_query',
                          'search_query_score', 'search_query_volume',
                          'impressions_total', 'impressions_asin', 'impressions_share',
                          'clicks_total', 'clicks_total_rate', 'clicks_asin', 'clicks_asin_share',
                          'cart_adds_total', 'cart_adds_total_rate', 'cart_adds_asin', 'cart_adds_asin_share',
                          'purchases_total', 'purchases_total_rate', 'purchases_asin', 'purchases_asin_share'],
}
_KEYWORDS_SCHEMA = ['month', 'sku_id', 'sku_name', 'keyword',
                    'total_views', 'impression_pct', 'attributed_views']


def save_daily_csv(tables, path):
    """Write rumee_db_daily.csv. tables = {table_name: [rows]}.
    Returns (total_rows, min_date_str, max_date_str)."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for tname, cols in _DAILY_SCHEMAS.items():
            rows = tables.get(tname, [])
            w.writerow(['__table__'] + cols)
            for rec in rows:
                w.writerow([tname] + [rec.get(c, '') for c in cols])
    all_dates = [r['date'] for rows in tables.values() for r in rows if r.get('date')]
    d_min = min(all_dates) if all_dates else ''
    d_max = max(all_dates) if all_dates else ''
    total = sum(len(v) for v in tables.values())
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

def _key_norm(v):
    """Normalize an upsert dedup/sort key field to a stable string.
    Numeric IDs reloaded from CSV come back as floats (e.g. 22247405.0) while
    freshly-parsed rows hold them as strings ('22247405'). Coerce both to the
    same canonical string so dedup matches AND sorting never compares str vs
    float (which raises TypeError in Python 3)."""
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() else str(v)
    return str(v)

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
        # FK Ads reports — all start with "start time," metadata header
        if 'start time,' in hdr and 'campaign id' in hdr:
            if 'attributed_keyword' in hdr and 'keyword_match_type' in hdr:
                return 'FK_ADS_KW'
            if 'order_id' in hdr or 'advertised fsn id' in hdr:
                return 'FK_ADS_ORDERS'
            if 'placement type' in hdr:
                return 'FK_ADS_PLACEMENTS'
            if 'query' in hdr and 'adgroup id' in hdr:
                return 'FK_ADS_SEARCH'
            if 'listing id' in hdr and 'adgroup cpc' in hdr:
                return 'FK_ADS_OVERALL'
            if 'sku id' in hdr and ',date,' not in hdr:
                return 'FK_ADS_FSN'
            if ',date,' in hdr or '\ndate' in hdr or 'campaign id,campaign name,date' in hdr:
                return 'FK_ADS_DAILY'
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
        # FK Fulfilment Orders report: has 'Orders' sheet with 'order_item_id' column
        if 'orders' in [s.lower() for s in sheets]:
            try:
                ord_hdr = sniff_csv_header(path) if False else ''  # not a CSV
                df_peek = pd.read_excel(path, sheet_name='Orders', nrows=1)
                if 'order_item_id' in [str(c).lower() for c in df_peek.columns]:
                    return 'FK_ORDERS'
            except Exception:
                pass
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

_ME_STATUS_MAP = {
    'DELIVERED':     'Delivered',
    'RTO_COMPLETE':  'RTO',
    'CANCELLED':     'Cancelled',
    'LOST':          'Cancelled',
}

def process_meesho_orders(path, last_date_str):
    """
    Returns:
        monthly: {month: {gmv, orders, returns, ...}}
        skus:    {sku_id: {name, delivered, rto, gmv, avg_price, ...}}
        new_last_date: str
        order_rows: list of per-order dicts for Orders Ledger
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(path, dtype={'Order Date': str})

    # Parse dates
    df['_dt'] = pd.to_datetime(df['Order Date'], errors='coerce').dt.date
    before = len(df)
    df = df[df['_dt'].notna()]
    df_new = df[_dt_gt(df['_dt'], last_date)]
    df_skip = df[_dt_le(df['_dt'], last_date)]

    print(f"  ME Orders: {len(df_new)} new rows ({df_new['_dt'].min()} to {df_new['_dt'].max() if len(df_new) else 'N/A'}), "
          f"skipping {len(df_skip)} already-processed rows")
    if len(df_new) == 0:
        return {}, {}, last_date_str, []
    new_last = df_new['_dt'].max()

    status_col   = 'Reason for Credit Entry'
    price_col    = 'Supplier Discounted Price (Incl GST and Commision)'
    listed_col   = 'Supplier Listed Price (Incl. GST + Commission)'
    sku_col      = 'SKU'
    suborder_col = 'Sub Order No'
    qty_col      = 'Quantity'
    state_col    = 'Customer State'

    monthly    = {}
    skus       = {}
    order_rows = []

    for _, row in df_new.iterrows():
        status = str(row.get(status_col, '')).strip()
        mk     = month_key(str(row['_dt']))
        if not mk:
            continue
        price  = float(row.get(price_col, 0) or 0)
        gmv    = float(row.get(listed_col, 0) or 0)
        raw_sku = str(row.get(sku_col, '')).strip()
        sid, sname = me_sku_id(raw_sku)

        m = monthly.setdefault(mk, {'gmv':0,'orders':0,'returns':0})
        s = skus.setdefault(sid, {
            'name':sname,'type':'','delivered':0,'rto':0,'cancelled':0,
            'gmv':0,'prices':[],'orders':0
        })

        # Orders/GMV count every row that isn't CANCELLED/LOST -- NOT gated
        # on status=='DELIVERED' (Jaiswal, 2026-07-18: "nothing should be
        # tied to being Delivered for calculating the number of orders and
        # GMV" -- a cancelled/lost order never became a real sale, a
        # delivered/RTO'd/still-in-transit one did. Matches the same
        # exclusion `shippable_units` already uses for stock decrement,
        # active.md item #64/#65). `delivered`/`rto` stay separately tracked
        # sub-counts -- used only for return_rate below, which deliberately
        # keeps its own narrower "orders that reached a final outcome"
        # denominator (an in-transit order hasn't had a chance to RTO yet;
        # folding it into that denominator would artificially dilute the
        # rate, a different question than "did this become a real order").
        if status in ('CANCELLED', 'LOST'):
            s['cancelled'] += 1
        else:
            m['gmv']    += price
            m['orders'] += 1
            s['orders'] += 1
            s['gmv']    += price
            s['prices'].append(price)
            if status == 'DELIVERED':
                s['delivered'] += 1
            elif status == 'RTO_COMPLETE':
                m['returns'] += 1
                s['rto'] += 1
            # else: SHIPPED / READY_TO_SHIP / RTO_OFD / RTO_LOCKED /
            # RTO_INITIATED / HOLD -- still in transit, already counted above

        # Per-order row for Ledger (all statuses including in-transit)
        mapped_status = _ME_STATUS_MAP.get(status, 'In-Transit')
        suborder_id = str(row.get(suborder_col, '')).strip()
        order_rows.append({
            'order_id':   suborder_id,
            'order_date': str(row['_dt']),
            'platform':   'ME',
            'sku':        sid,
            'sku_name':   sname,
            'qty':        int(float(row.get(qty_col, 1) or 1)),
            'gmv':        round(gmv, 2),
            'settlement': round(price, 2),
            'commission': 0.0,
            'fixed_fee':        0.0,
            'collection_fee':   0.0,
            'shipping_fwd':     0.0,
            'shipping_rev':     0.0,
            'gst_on_fees':      0.0,
            'tcs':              0.0,
            'tds':              0.0,
            'penalty':          0.0,
            'status':     mapped_status,
            'zone':       str(row.get(state_col, '')).strip(),
            'is_shopsy':  '',
        })

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
        # Broad count (all non-cancelled/lost orders) -- was delivered+rto,
        # which undercounted every still-in-transit order. return_rate above
        # deliberately keeps its own narrower delivered+rto denominator.
        s['total_orders'] = s['orders']

    return monthly, skus, str(new_last), order_rows

# ─── Meesho Returns ───────────────────────────────────────────────────────────

def process_meesho_returns(path, last_date_str):
    """
    Bucketed by DELIVERED DATE (when the returned item actually made it back —
    i.e. closure) — active.md item #70, 2026-07-20. Falls back to Return
    Created Date for a row that has no Delivered Date yet (an in-progress
    return isn't closed; it'll carry a real Delivered Date once the rolling
    report snapshot catches up, so nothing is silently dropped, only bucketed
    provisionally under its start date until then).

    Returns:
        sku_returns: {sku_id: {cust_returns, incomplete, wrong_product, quality}}
        reasons:     {reason_str: count}
        new_last_date: str
        suborder_reason_index: {suborder_id: return_reason_str}
        suborder_awb_index: {suborder_id: awb_number} -- from the report's own
            "AWB Number" column, used by the Orders Ledger to resolve a Return
            Receipts scan that only captured the AWB (not the Suborder Number)
            back to the real suborder_id.
        suborder_sku_index: {suborder_id: sku_id} -- the report's own "SKU"
            column, resolved via me_sku_id() (same resolution already used for
            sku_returns above). Added 2026-07-21 (dashboard memory active.md
            item #72) so the Returns Scanner's SKU lookup can prefer the
            RETURN's own reported SKU over the order-placement-time guess,
            once this specific return has actually synced.
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
        df = pd.read_csv(path, skiprows=7, engine='python', on_bad_lines='skip')
    else:
        df = pd.read_csv(path, skiprows=header_idx, engine='python', on_bad_lines='skip')

    # Strip quotes from column names
    df.columns = [c.strip('"').strip() for c in df.columns]

    # Date column — prefer 'Delivered Date' (closure); fall back to
    # 'Return Created Date' (row not yet delivered back), then 'Dispatch Date'
    # (active.md item #70, 2026-07-20 — was 'Return Created Date' only).
    # Fallback is per-ROW, not per-file: a file can have BOTH columns, with
    # some rows delivered (Delivered Date populated) and some still
    # in-transit (Delivered Date blank, Return Created Date populated) --
    # picking a single column for the whole file would silently drop every
    # in-transit row via the notna() filter below (independent code review
    # finding, 2026-07-20 — the file-level pick previously shipped here
    # contradicted this exact docstring's own "nothing is silently dropped"
    # claim).
    delivered_col = next((c for c in df.columns if 'Delivered Date' in c), None)
    created_col   = next((c for c in df.columns if 'Return Created Date' in c), None)
    dispatch_col  = next((c for c in df.columns if 'Dispatch Date' in c), None)
    sku_col  = next((c for c in df.columns if c == 'SKU'), 'SKU')
    type_col = next((c for c in df.columns if 'Type of Return' in c), None)
    reason_col = next((c for c in df.columns if 'Detailed Return Reason' in c), None)
    sub_reason_col = next((c for c in df.columns if 'Return Reason' in c and 'Detailed' not in c), None)

    _dt_delivered = pd.to_datetime(df[delivered_col], errors='coerce').dt.date if delivered_col else pd.Series(pd.NaT, index=df.index)
    _dt_created   = pd.to_datetime(df[created_col],   errors='coerce').dt.date if created_col   else pd.Series(pd.NaT, index=df.index)
    _dt_dispatch  = pd.to_datetime(df[dispatch_col],  errors='coerce').dt.date if dispatch_col  else pd.Series(pd.NaT, index=df.index)
    df['_dt'] = _dt_delivered.combine_first(_dt_created).combine_first(_dt_dispatch)
    before = len(df)
    df = df[df['_dt'].notna()]
    df_new = df[_dt_gt(df['_dt'], last_date)]
    df_skip = df[_dt_le(df['_dt'], last_date)]
    new_last = df['_dt'].max() if len(df) else last_date

    print(f"  ME Returns: {len(df_new)} new rows ({df_new['_dt'].min() if len(df_new) else 'N/A'} to "
          f"{df_new['_dt'].max() if len(df_new) else 'N/A'}), skipping {len(df_skip)}")
    if len(df_new) == 0:
        return {}, {}, str(new_last), {}, {}, {}

    sku_returns           = {}
    reasons               = {}
    suborder_reason_index = {}
    suborder_awb_index    = {}
    suborder_sku_index    = {}

    suborder_col_r = next((c for c in df.columns if 'Suborder Number' in c or 'Sub Order No' in c), None)
    awb_col_r      = next((c for c in df.columns if 'AWB Number' in c), None)

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

        # Build suborder → return_reason index for Ledger
        r_label = reason_detail if reason_detail and reason_detail not in ('NA', 'nan', '') else reason_sub
        if suborder_col_r:
            sub_id = str(row.get(suborder_col_r, '')).strip().strip('"')
            if sub_id and sub_id not in ('nan', ''):
                suborder_reason_index[sub_id] = r_label
                if awb_col_r:
                    awb_val = str(row.get(awb_col_r, '')).strip().strip('"')
                    if awb_val and awb_val not in ('nan', ''):
                        suborder_awb_index[sub_id] = awb_val
                if sid:
                    suborder_sku_index[sub_id] = sid

    return sku_returns, reasons, str(new_last), suborder_reason_index, suborder_awb_index, suborder_sku_index

# ─── Meesho Payments ──────────────────────────────────────────────────────────

def _unwrap_double_zipped_xlsx(path):
    """
    Some Meesho payment exports arrive as a zip that wraps a single real
    .xlsx file as its only entry (instead of being the xlsx itself), so
    pandas can't identify them as xlsx. Detect that shape and return a path
    to the real inner .xlsx (extracted next to the original); otherwise
    return path unchanged.
    """
    import zipfile
    try:
        with zipfile.ZipFile(path) as zf:
            names = zf.namelist()
            if '[Content_Types].xml' in names:
                return path  # already a real xlsx
            if len(names) == 1 and names[0].lower().endswith('.xlsx'):
                inner_path = Path(str(path) + '.unwrapped.xlsx')
                inner_path.write_bytes(zf.read(names[0]))
                return inner_path
    except zipfile.BadZipFile:
        pass
    return path


def process_meesho_payments(path, last_date_str, ads_last_date_str=None):
    """
    Handles single-sheet (legacy) and multi-sheet (v2) Meesho payment files.

    Multi-sheet format:
        Sheet 'Order Payments'          -> settlement data
        Sheet 'Ads Cost'                -> ads spend (same as standalone ME_ADS)
        Sheet 'Compensation and Recovery' -> logged, not stored yet

    Positional columns (Order Payments sheet):
        col 0  = Sub Order No (order-status registry key, active.md item #66)
        col 1  = Order Date (kept for revenue-month bucketing only)
        col 7  = Live Order Status -- 'Delivered'/'RTO'/'Return' (active.md
                 item #66, 2026-07-18): confirmed against a real live sample
                 that a row only appears here once an order has reached one
                 of these 3 FINAL outcomes -- this is the authoritative
                 status, always overrides whatever the Orders file guessed.
        col 12 = Payment Date (watermark + new-row filter — Order Date lags due to
                 settlement delay and is not monotonic across files, so it cannot be
                 used for dedup; Payment Date equals the file's own report date)
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
        monthly_sett:      {month: settlement_float}
        monthly_ads:       {month: ad_spend_float}  -- empty if no Ads Cost sheet
        pay_new_last:      str  -- new last date for settlement
        ads_new_last:      str  -- new last date for ads (unchanged if no ads sheet)
        order_statuses:    {sub_order_id: 'delivered'|'rto'|'return'} for every
                            new row with a recognized Live Order Status --
                            caller overwrites the persisted order-status
                            registry with these (active.md item #66).
        order_settlements: {sub_order_id: settlement_float} -- real per-order
                            settlement amount (col 13), for every new row.
                            Previously only ever summed into monthly_sett and
                            discarded per-row; now also kept per order so the
                            Orders Ledger can show real settlement instead of
                            the order-price estimate once this order's
                            Payments row arrives (active.md item #67,
                            2026-07-19).
    """
    if ads_last_date_str is None:
        ads_last_date_str = last_date_str

    last_date     = datetime.strptime(last_date_str,     '%Y-%m-%d').date()
    ads_last_date = datetime.strptime(ads_last_date_str, '%Y-%m-%d').date()

    xl = pd.ExcelFile(_unwrap_double_zipped_xlsx(path))
    sheet_names = xl.sheet_names

    # ── Find the order-payments sheet ────────────────────────────────────────
    orders_sheet = next(
        (s for s in sheet_names if 'order' in s.lower()),
        sheet_names[0]   # fallback: first sheet
    )

    # ── Process settlement data ───────────────────────────────────────────────
    df = xl.parse(orders_sheet, header=[0, 1, 2])
    print(f"  ME Payments: {len(df.columns)} cols, {len(df)} rows (sheet={orders_sheet})")
    if len(df.columns) < 14:
        df = xl.parse(orders_sheet, header=[0, 1])
        print(f"  ME Payments: fell back to 2-row header → {len(df.columns)} cols")
    suborder_ids = df.iloc[:, 0].astype(str).str.strip()                    # col 0  = Sub Order No
    order_dates = pd.to_datetime(df.iloc[:, 1],  errors='coerce').dt.date   # col 1  = Order Date (revenue-month bucketing)
    live_status = df.iloc[:, 7].astype(str).str.strip()                    # col 7  = Live Order Status
    pay_dates   = pd.to_datetime(df.iloc[:, 12], errors='coerce').dt.date   # col 12 = Payment Date (watermark + filter)
    setts       = pd.to_numeric(df.iloc[:, 13], errors='coerce').fillna(0)  # col 13 = Settlement

    valid  = pay_dates.notna()   # gate on Payment Date — the reliable, monotonic-per-file column
    df2    = pd.DataFrame({
        '_dt': pay_dates[valid], 'order_dt': order_dates[valid], 'sett': setts[valid],
        'suborder_id': suborder_ids[valid], 'live_status': live_status[valid],
    })
    df_new = df2[_dt_gt(df2['_dt'], last_date)]
    pay_new_last = df_new['_dt'].max() if len(df_new) else last_date

    print(f"  ME Payments (orders): {len(df_new)} new rows, "
          f"skipping {len(df2) - len(df_new)}")

    monthly_sett = {}
    # Live Order Status -> canonical registry status (active.md item #66).
    # Anything not in this map (blank, or an unrecognized value) is left
    # out of order_statuses entirely -- never guessed, matches Jaiswal's
    # "never guess/fuzzy-match" standing instruction elsewhere in this file.
    _ME_LIVE_STATUS_MAP = {'Delivered': 'delivered', 'RTO': 'rto', 'Return': 'return'}
    order_statuses = {}
    order_settlements = {}
    for _, row in df_new.iterrows():
        mk = month_key(str(row['order_dt']))   # bucket by Order Date, unchanged semantics
        if not mk:
            mk = month_key(str(row['_dt']))   # Order Date missing/malformed — fall back to Payment Date so settlement isn't silently dropped
        if mk:
            monthly_sett[mk] = monthly_sett.get(mk, 0) + float(row['sett'])
        canon = _ME_LIVE_STATUS_MAP.get(row['live_status'])
        if canon and row['suborder_id']:
            order_statuses[row['suborder_id']] = canon
        if row['suborder_id']:
            order_settlements[row['suborder_id']] = round(float(row['sett']), 2)
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
            df_ads = xl.parse(ads_sheet, header=[0, 1, 2])
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
            _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"ME Payments ads sheet parse failed: {e}",
                                   'impact': "this file's Meesho ad-spend figures were skipped this run — ad spend numbers may be understated until a future file recovers them"})

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

    return monthly_sett, monthly_ads, str(pay_new_last), str(ads_new_last), order_statuses, order_settlements

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
    df_new = df2[_dt_gt(df2['_dt'], last_date)]
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
        col 2  = Payment Date (watermark + new-row filter — Order Date lags due to
                 settlement delay and is not monotonic across files, so it cannot be
                 used for dedup; Payment Date equals the file's own report date)
        col 3  = Bank Settlement Value
        col 9  = Sale Amount
        col 55 = Order Date (kept for revenue-month bucketing and the Orders Ledger only)
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
    df = xl.parse(orders_sheet, header=[0, 1, 2])
    print(f"  FK Payments: {len(df.columns)} cols, {len(df)} rows (sheet={orders_sheet})")
    if len(df.columns) < 63:
        df = xl.parse(orders_sheet, header=[0, 1])
        print(f"  FK Payments: fell back to 2-row header → {len(df.columns)} cols")

    order_dates = pd.to_datetime(df.iloc[:, 55], errors='coerce').dt.date  # col 55 = Order Date (bucketing + Ledger, unchanged)
    pay_dates   = pd.to_datetime(df.iloc[:, 2],  errors='coerce').dt.date  # col 2  = Payment Date (watermark + filter)
    skus_raw = df.iloc[:, 58].astype(str)
    sale_amt = pd.to_numeric(df.iloc[:, 9],  errors='coerce').fillna(0)
    sett_amt = pd.to_numeric(df.iloc[:, 3],  errors='coerce').fillna(0)
    ret_type = df.iloc[:, 62].astype(str)

    zone_raw    = df.iloc[:, 53].astype(str)
    shopsy_raw  = df.iloc[:, 63].astype(str)
    revship_raw = pd.to_numeric(df.iloc[:, 26], errors='coerce').fillna(0)

    # Search for ledger-specific columns by header name (flexible — FK sometimes renames)
    def _fkp_num(keywords):
        for col in df.columns:
            s = ' '.join(str(c).lower() for c in (col if isinstance(col, tuple) else (col,)))
            if any(k in s for k in keywords):
                return pd.to_numeric(df[col], errors='coerce').fillna(0)
        return pd.Series(0.0, index=df.index)

    def _fkp_str(keywords):
        for col in df.columns:
            s = ' '.join(str(c).lower() for c in (col if isinstance(col, tuple) else (col,)))
            if any(k in s for k in keywords):
                return df[col].astype(str)
        return pd.Series('', index=df.index)

    order_id_raw    = _fkp_str(['order id', 'order_id', 'orderid'])
    commission_raw  = _fkp_num(['commission', 'marketplace fee', 'seller fee'])
    fixed_fee_raw   = _fkp_num(['fixed fee', 'fixed_fee', 'fixedfee'])
    coll_fee_raw    = _fkp_num(['collection fee', 'payment gateway', 'pg fee', 'collection_fee'])
    ship_fwd_raw    = _fkp_num(['forward shipping', 'shipping charge', 'forward ship', 'forward_ship'])
    gst_raw         = _fkp_num(['gst on commission', 'gst on fees', 'gst on marketplace', 'igst', 'cgst'])
    tcs_raw         = _fkp_num(['tcs', 'tax collected at source'])
    tds_raw         = _fkp_num(['tds', 'tax deducted at source'])
    penalty_raw     = _fkp_num(['penalty', 'other deduction', 'penalty_amount'])

    valid = pay_dates.notna()   # gate on Payment Date — the reliable, monotonic-per-file column
    df2   = pd.DataFrame({
        '_dt': pay_dates[valid], 'order_dt': order_dates[valid], 'sku': skus_raw[valid], 'sale': sale_amt[valid],
        'sett': sett_amt[valid], 'ret': ret_type[valid],
        'zone': zone_raw[valid], 'shopsy': shopsy_raw[valid],
        'revship': revship_raw[valid],
        'order_id':   order_id_raw[valid],
        'commission': commission_raw[valid], 'fixed_fee': fixed_fee_raw[valid],
        'coll_fee':   coll_fee_raw[valid],  'ship_fwd':  ship_fwd_raw[valid],
        'gst':        gst_raw[valid],       'tcs':       tcs_raw[valid],
        'tds':        tds_raw[valid],       'penalty':   penalty_raw[valid],
    })
    _last_ts = pd.Timestamp(last_date)
    df_new   = df2[pd.to_datetime(df2['_dt'], errors='coerce') > _last_ts]
    pay_new_last = df_new['_dt'].max() if len(df_new) else last_date

    print(f"  FK Payments (orders): {len(df_new)} new rows "
          f"({df_new['_dt'].min() if len(df_new) else 'N/A'} to "
          f"{df_new['_dt'].max() if len(df_new) else 'N/A'}), "
          f"skipping {len(df2) - len(df_new)}")

    monthly        = {}
    skus           = {}
    monthly_shopsy = {}   # {month: {shopsy_orders, shopsy_revenue}}
    sku_revship    = {}   # {sku_id: reverse_shipping_total}
    zone_counts    = {}   # {zone: {orders, revenue, returns}}
    order_rows     = []   # individual order rows for Orders Ledger
    # order_statuses (active.md item #66, 2026-07-18): {order_id: canonical
    # status} for the persisted order-status registry -- always overrides
    # whatever the Orders file guessed, confirmed via a real live sample
    # that Return Type only appears once an order reaches a final outcome.
    order_statuses = {}

    for _, row in df_new.iterrows():
        mk = month_key(str(row['order_dt']))   # bucket by Order Date, unchanged semantics
        if not mk:
            mk = month_key(str(row['_dt']))   # Order Date missing/malformed — fall back to Payment Date so the row isn't silently dropped
        if not mk:
            continue
        sale      = float(row['sale'])
        sett      = float(row['sett'])
        is_return = row['ret'] in ('Customer Return', 'Logistics Return')
        is_shopsy = str(row['shopsy']).strip().lower() == 'yes'
        zone      = str(row['zone']).strip()
        revship   = abs(float(row['revship']))
        raw_sku   = str(row['sku']).strip()
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

        # Shopsy tracking
        if is_shopsy and sale > 0:
            sh = monthly_shopsy.setdefault(mk, {'shopsy_orders': 0, 'shopsy_revenue': 0.0})
            sh['shopsy_orders']  += 1
            sh['shopsy_revenue'] += sale

        # Reverse shipping cost per SKU
        if revship > 0:
            sku_revship[sid] = round(sku_revship.get(sid, 0) + revship, 2)

        # Shipping zone distribution
        if zone and zone not in ('nan', 'None', ''):
            z = zone_counts.setdefault(zone, {'orders': 0, 'revenue': 0.0, 'returns': 0})
            if sale > 0:
                z['orders']  += 1
                z['revenue'] += sale
            if is_return:
                z['returns'] += 1

        # Orders Ledger — individual row
        _status = ('Returned-Customer' if row['ret'] == 'Customer Return'
                   else 'RTO' if row['ret'] == 'Logistics Return'
                   else 'Delivered' if sale > 0 else '')
        _oid = str(row.get('order_id', '') or '').strip().strip('"')
        _canon = ({'Returned-Customer': 'return', 'RTO': 'rto', 'Delivered': 'delivered'}
                   .get(_status))
        if _canon and _oid:
            order_statuses[_oid] = _canon
        order_rows.append({
            'order_id':      str(row.get('order_id', '') or '').strip().strip('"'),
            'order_date':    str(row['order_dt']) if pd.notna(row['order_dt']) else str(row['_dt']),
            'platform':      'FK',
            'sku':           sid,
            'qty':           1,
            'gmv':           round(sale, 2),
            'settlement':    round(sett, 2),
            'commission':    round(abs(float(row.get('commission', 0) or 0)), 2),
            'fixed_fee':     round(abs(float(row.get('fixed_fee', 0) or 0)), 2),
            'collection_fee':round(abs(float(row.get('coll_fee', 0) or 0)), 2),
            'shipping_fwd':  round(abs(float(row.get('ship_fwd', 0) or 0)), 2),
            'shipping_rev':  round(revship, 2),
            'gst_on_fees':   round(abs(float(row.get('gst', 0) or 0)), 2),
            'tcs':           round(abs(float(row.get('tcs', 0) or 0)), 2),
            'tds':           round(abs(float(row.get('tds', 0) or 0)), 2),
            'penalty':       round(abs(float(row.get('penalty', 0) or 0)), 2),
            'status':        _status,
            'zone':          zone,
            'is_shopsy':     'Y' if is_shopsy else '',
        })

    for m in monthly.values():
        m['gmv']        = round(m['gmv'], 2)
        m['settlement'] = round(m['settlement'], 2)
    for s in skus.values():
        s['gmv']        = round(s['gmv'], 2)
        s['settlement'] = round(s['settlement'], 2)
    for sh in monthly_shopsy.values():
        sh['shopsy_revenue'] = round(sh['shopsy_revenue'], 2)
    for z in zone_counts.values():
        z['revenue'] = round(z['revenue'], 2)

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
            _run_warnings.append({'file': str(path), 'type': 'FK', 'reason': f"FK Payments ads sheet parse failed: {e}",
                                   'impact': "this file's Flipkart ad-spend figures were skipped this run — ad spend numbers may be understated until a future file recovers them"})

    # Log GST sheet if present
    gst_sheet = next((s for s in sheet_names if 'gst' in s.lower()), None)
    if gst_sheet:
        try:
            df_gst = xl.parse(gst_sheet)
            print(f"  FK Payments (GST):    {len(df_gst)} rows (logged only, not stored)")
        except Exception:
            pass

    return monthly, skus, monthly_ads, monthly_shopsy, sku_revship, zone_counts, str(pay_new_last), str(ads_new_last), order_rows, order_statuses

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
    df_new = df2[_dt_gt(df2['_dt'], last_date)]
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


# ─── Flipkart Ads — Consolidated Daily Report ─────────────────────────────────

def _fk_ads_date_from_header(path):
    """Read Start Time from row 0 of an FK Ads CSV (format: 'Start Time, YYYY-MM-DD ...')."""
    try:
        with open(path, encoding='utf-8', errors='replace') as fh:
            first_line = fh.readline()
        # "Start Time, 2026-06-18 00:00:00"
        parts = first_line.split(',', 1)
        if len(parts) == 2:
            return str(pd.to_datetime(parts[1].strip(), errors='coerce').date())
    except Exception:
        pass
    # Fall back to date in filename
    m = re.search(r'(\d{4}-\d{2}-\d{2})(?!.*\d{4}-\d{2}-\d{2})', path.stem)
    return m.group(1) if m else TODAY


def process_fk_ads_daily(path):
    """
    Consolidated Daily Report — per-campaign daily performance.
    Columns: Campaign ID, Campaign Name, Date, Ad Spend, Views, Clicks,
             Total converted units, Total Revenue (Rs.), ROI
    Returns: list of row dicts for fk_ads_daily table.
    """
    df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]

    date_col  = next((c for c in df.columns if c.lower() == 'date'), None)
    camp_id   = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    camp_name = next((c for c in df.columns if 'campaign name' in c.lower()), None)
    spend_col = next((c for c in df.columns if 'ad spend' in c.lower()), None)
    rev_col   = next((c for c in df.columns if 'total revenue' in c.lower()), None)
    views_col = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col= next((c for c in df.columns if c.lower() == 'clicks'), None)
    conv_col  = next((c for c in df.columns if 'converted units' in c.lower()), None)

    if not camp_id or not spend_col:
        print(f"  FK Ads Daily: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        cid = str(row.get(camp_id, '')).strip()
        if not cid or cid.lower() in ('nan', ''):
            continue
        dt = str(pd.to_datetime(row.get(date_col, ''), errors='coerce').date()) \
             if date_col else _fk_ads_date_from_header(path)
        if dt == 'NaT' or not dt:
            continue
        spend   = float(row.get(spend_col, 0) or 0)
        revenue = float(row.get(rev_col,   0) or 0) if rev_col else 0.0
        roas    = round(revenue / spend, 4) if spend else 0.0
        rows.append({
            'date':          dt,
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_name, '')).strip() if camp_name else '',
            'ad_spend':      round(spend, 2),
            'revenue':       round(revenue, 2),
            'views':         int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':        int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'conversions':   int(float(row.get(conv_col,   0) or 0)) if conv_col   else 0,
            'roas':          roas,
        })

    print(f"  FK Ads Daily: {len(rows)} campaign-day rows")
    return rows


# ─── Flipkart Ads — Consolidated FSN (SKU-level) Report ──────────────────────

def process_fk_ads_fsn(path):
    """
    Consolidated FSN Report — per-SKU aggregate (no Date column; use header).
    Columns: Campaign ID, Campaign Name, Sku Id, Product Name, Views, Clicks,
             Direct Units Sold, Indirect Units Sold, Total Revenue (Rs.), Ad Spend, ROI
    Returns: list of row dicts for fk_ads_sku table.
    """
    report_date = _fk_ads_date_from_header(path)
    df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]

    camp_id   = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    camp_name = next((c for c in df.columns if 'campaign name' in c.lower()), None)
    sku_col   = next((c for c in df.columns if 'sku id' in c.lower()), None)
    name_col  = next((c for c in df.columns if 'product name' in c.lower()), None)
    views_col = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col= next((c for c in df.columns if c.lower() == 'clicks'), None)
    du_col    = next((c for c in df.columns if 'direct units' in c.lower()), None)
    iu_col    = next((c for c in df.columns if 'indirect units' in c.lower()), None)
    rev_col   = next((c for c in df.columns if 'total revenue' in c.lower()), None)
    spend_col = next((c for c in df.columns if 'ad spend' in c.lower()), None)

    if not sku_col or not spend_col:
        print(f"  FK Ads FSN: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        raw_sku = str(row.get(sku_col, '')).strip()
        if not raw_sku or raw_sku.lower() in ('nan', ''):
            continue
        sid, sname = fk_sku_id(raw_sku)
        if name_col and str(row.get(name_col, '')).strip() not in ('', 'nan'):
            sname = str(row.get(name_col, '')).strip()
        spend   = float(row.get(spend_col, 0) or 0)
        revenue = float(row.get(rev_col,   0) or 0) if rev_col else 0.0
        du      = int(float(row.get(du_col, 0) or 0)) if du_col else 0
        iu      = int(float(row.get(iu_col, 0) or 0)) if iu_col else 0
        roas    = round(revenue / spend, 4) if spend else 0.0
        rows.append({
            'date':          report_date,
            'campaign_id':   str(row.get(camp_id, '')).strip() if camp_id else '',
            'campaign_name': str(row.get(camp_name, '')).strip() if camp_name else '',
            'sku_id':        sid,
            'sku_name':      sname,
            'views':         int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':        int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'units_sold':    du + iu,
            'revenue':       round(revenue, 2),
            'ad_spend':      round(spend, 2),
            'roas':          roas,
        })

    print(f"  FK Ads FSN: {len(rows)} SKU rows for {report_date}")
    return rows


# ─── Flipkart Ads — Keyword Report ───────────────────────────────────────────

def process_fk_ads_kw(path):
    """
    Ads Keyword Report — 4 header rows before data.
    Columns: Campaign ID, Campaign Name, attributed_keyword, keyword_match_type,
             Views, Clicks, SUM(cost), ROI
    Returns: list of row dicts for fk_ads_kw table.
    """
    report_date = _fk_ads_date_from_header(path)
    df = pd.read_csv(path, skiprows=4, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]

    camp_id   = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    camp_name = next((c for c in df.columns if 'campaign name' in c.lower()), None)
    kw_col    = next((c for c in df.columns if 'keyword' in c.lower() and 'attributed' in c.lower()), None)
    mt_col    = next((c for c in df.columns if 'match_type' in c.lower() or 'match type' in c.lower()), None)
    views_col = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col= next((c for c in df.columns if c.lower() == 'clicks'), None)
    spend_col = next((c for c in df.columns if 'sum(cost)' in c.lower() or c.lower() == 'spend'), None)

    if not kw_col or not camp_id:
        print(f"  FK Ads KW: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        kw = str(row.get(kw_col, '')).strip()
        cid = str(row.get(camp_id, '')).strip()
        if not kw or kw.lower() in ('nan', '') or not cid or cid.lower() in ('nan', ''):
            continue
        rows.append({
            'date':          report_date,
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_name, '')).strip() if camp_name else '',
            'keyword':       kw,
            'match_type':    str(row.get(mt_col, '')).strip() if mt_col else '',
            'views':         int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':        int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'spend':         round(float(row.get(spend_col, 0) or 0), 2) if spend_col else 0.0,
        })

    print(f"  FK Ads KW: {len(rows)} keyword rows for {report_date}")
    return rows


# ─── Flipkart Ads — Placement Performance Report ─────────────────────────────

def process_fk_ads_placements(path):
    """
    Placement Performance Report — spend by placement type (Product Page / Search).
    Columns: Campaign ID, Campaign Name, AdGroup Name, Placement Type, Views, Clicks,
             Ad Spend, Direct Units Sold, Indirect Units Sold, Direct Revenue, Indirect Revenue
    Returns: list of row dicts for fk_ads_placements table.
    """
    report_date = _fk_ads_date_from_header(path)
    df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]

    camp_id    = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    camp_name  = next((c for c in df.columns if 'campaign name' in c.lower()), None)
    place_col  = next((c for c in df.columns if 'placement type' in c.lower()), None)
    views_col  = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col = next((c for c in df.columns if c.lower() == 'clicks'), None)
    spend_col  = next((c for c in df.columns if 'ad spend' in c.lower()), None)
    du_col     = next((c for c in df.columns if 'direct units' in c.lower()), None)
    iu_col     = next((c for c in df.columns if 'indirect units' in c.lower()), None)
    dr_col     = next((c for c in df.columns if 'direct revenue' in c.lower()), None)
    ir_col     = next((c for c in df.columns if 'indirect revenue' in c.lower()), None)

    if not camp_id or not place_col:
        print(f"  FK Ads Placements: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        cid = str(row.get(camp_id, '')).strip()
        pt  = str(row.get(place_col, '')).strip()
        if not cid or cid.lower() in ('nan', '') or not pt or pt.lower() in ('nan', ''):
            continue
        du  = int(float(row.get(du_col, 0) or 0)) if du_col else 0
        iu  = int(float(row.get(iu_col, 0) or 0)) if iu_col else 0
        dr  = float(row.get(dr_col, 0) or 0) if dr_col else 0.0
        ir  = float(row.get(ir_col, 0) or 0) if ir_col else 0.0
        spend   = float(row.get(spend_col, 0) or 0) if spend_col else 0.0
        revenue = round(dr + ir, 2)
        rows.append({
            'date':          report_date,
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_name, '')).strip() if camp_name else '',
            'placement_type': pt,
            'views':         int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':        int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'ad_spend':      round(spend, 2),
            'units_sold':    du + iu,
            'revenue':       revenue,
            'roas':          round(revenue / spend, 4) if spend else 0.0,
        })

    print(f"  FK Ads Placements: {len(rows)} placement rows for {report_date}")
    return rows


# ─── Flipkart Ads — Overall Performance Report (per-listing/SKU) ─────────────

def process_fk_ads_overall(path):
    """
    Overall Performance Report — per-SKU with Listing ID, CPC, direct/indirect split.
    Columns: Campaign ID, AdGroup Name, Listing ID, Product Name, Sku Id, AdGroup CPC,
             Views, Clicks, Total converted units, Ad Spend, Total Revenue,
             Direct Units Sold, Direct Revenue, Indirect Units Sold, Indirect Revenue, ROI
    Returns: list of row dicts for fk_ads_overall table.
    """
    report_date = _fk_ads_date_from_header(path)
    df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]

    camp_id    = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    listing_col= next((c for c in df.columns if 'listing id' in c.lower()), None)
    name_col   = next((c for c in df.columns if 'product name' in c.lower()), None)
    sku_col    = next((c for c in df.columns if 'sku id' in c.lower()), None)
    cpc_col    = next((c for c in df.columns if 'adgroup cpc' in c.lower()), None)
    views_col  = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col = next((c for c in df.columns if c.lower() == 'clicks'), None)
    conv_col   = next((c for c in df.columns if 'total converted' in c.lower()), None)
    spend_col  = next((c for c in df.columns if 'ad spend' in c.lower()), None)
    rev_col    = next((c for c in df.columns if 'total revenue' in c.lower()), None)
    du_col     = next((c for c in df.columns if 'direct units' in c.lower()), None)
    dr_col     = next((c for c in df.columns if 'direct revenue' in c.lower()), None)
    iu_col     = next((c for c in df.columns if 'indirect units' in c.lower()), None)
    ir_col     = next((c for c in df.columns if 'indirect revenue' in c.lower()), None)

    if not camp_id or not sku_col:
        print(f"  FK Ads Overall: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        raw_sku = str(row.get(sku_col, '')).strip()
        cid     = str(row.get(camp_id, '')).strip()
        if not raw_sku or raw_sku.lower() in ('nan', '') or not cid or cid.lower() in ('nan', ''):
            continue
        sid, sname = fk_sku_id(raw_sku)
        if name_col and str(row.get(name_col, '')).strip() not in ('', 'nan'):
            sname = str(row.get(name_col, '')).strip()
        spend   = float(row.get(spend_col, 0) or 0) if spend_col else 0.0
        revenue = float(row.get(rev_col,   0) or 0) if rev_col  else 0.0
        du = int(float(row.get(du_col, 0) or 0)) if du_col else 0
        iu = int(float(row.get(iu_col, 0) or 0)) if iu_col else 0
        dr = float(row.get(dr_col, 0) or 0) if dr_col else 0.0
        ir = float(row.get(ir_col, 0) or 0) if ir_col else 0.0
        rows.append({
            'date':           report_date,
            'campaign_id':    cid,
            'sku_id':         sid,
            'sku_name':       sname,
            'listing_id':     str(row.get(listing_col, '')).strip() if listing_col else '',
            'cpc':            float(row.get(cpc_col, 0) or 0) if cpc_col else 0.0,
            'views':          int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':         int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'units_direct':   du,
            'units_indirect': iu,
            'revenue_direct': round(dr, 2),
            'revenue_indirect': round(ir, 2),
            'ad_spend':       round(spend, 2),
            'revenue':        round(revenue, 2),
            'roas':           round(revenue / spend, 4) if spend else 0.0,
        })

    print(f"  FK Ads Overall: {len(rows)} SKU rows for {report_date}")
    return rows


# ─── Flipkart Ads — Search Term Report ───────────────────────────────────────

def process_fk_ads_search(path):
    """
    Search Term Report — which queries triggered ads, with spend and conversions.
    Columns: AdGroup ID, AdGroup Name, Campaign ID, Campaign Name, Query, Views, Clicks,
             Average CPC, CTR, Direct Units Sold, Indirect Units Sold,
             Direct Revenue, Indirect Revenue, ROI, SUM(cost)
    Returns: list of row dicts for fk_ads_search table.
    Note: ' Direct Units Sold' has a leading space in the header.
    """
    report_date = _fk_ads_date_from_header(path)
    df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    df.columns = [str(c).strip() for c in df.columns]  # strip leading/trailing spaces

    camp_id    = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    camp_name  = next((c for c in df.columns if 'campaign name' in c.lower()), None)
    query_col  = next((c for c in df.columns if c.lower() == 'query'), None)
    views_col  = next((c for c in df.columns if c.lower() == 'views'), None)
    clicks_col = next((c for c in df.columns if c.lower() == 'clicks'), None)
    spend_col  = next((c for c in df.columns if 'sum(cost)' in c.lower()), None)
    du_col     = next((c for c in df.columns if 'direct units' in c.lower()), None)
    iu_col     = next((c for c in df.columns if 'indirect units' in c.lower()), None)
    dr_col     = next((c for c in df.columns if 'direct revenue' in c.lower()), None)
    ir_col     = next((c for c in df.columns if 'indirect revenue' in c.lower()), None)

    if not query_col or not camp_id:
        print(f"  FK Ads Search: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        query = str(row.get(query_col, '')).strip()
        cid   = str(row.get(camp_id,   '')).strip()
        if not query or query.lower() in ('nan', '') or not cid or cid.lower() in ('nan', ''):
            continue
        du  = int(float(row.get(du_col, 0) or 0)) if du_col else 0
        iu  = int(float(row.get(iu_col, 0) or 0)) if iu_col else 0
        dr  = float(row.get(dr_col, 0) or 0) if dr_col else 0.0
        ir  = float(row.get(ir_col, 0) or 0) if ir_col else 0.0
        spend = float(row.get(spend_col, 0) or 0) if spend_col else 0.0
        rows.append({
            'date':          report_date,
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_name, '')).strip() if camp_name else '',
            'query':         query,
            'views':         int(float(row.get(views_col,  0) or 0)) if views_col  else 0,
            'clicks':        int(float(row.get(clicks_col, 0) or 0)) if clicks_col else 0,
            'spend':         round(spend, 2),
            'units_sold':    du + iu,
            'revenue':       round(dr + ir, 2),
        })

    print(f"  FK Ads Search: {len(rows)} query rows for {report_date}")
    return rows


# ─── Flipkart Ads — Campaign Order Report ────────────────────────────────────

def process_fk_ads_orders(path):
    """
    Campaign Order Report — individual orders attributed to ads.
    Columns: Campaign ID, AdGroup Name, Listing ID, Product Name, Advertised FSN ID,
             Date, order_id, AdGroup CPC, Purchased FSN ID, Total Revenue,
             Direct Units Sold, Indirect Units Sold
    Returns: list of row dicts for fk_ads_order_items table.
    """
    try:
        df = pd.read_csv(path, skiprows=2, encoding='utf-8', encoding_errors='replace', on_bad_lines='skip', engine='python')
    except pd.errors.EmptyDataError:
        # A genuinely empty (0-byte, or nothing left after skiprows=2) export
        # is a real possibility from the source report, not a parsing bug —
        # treat it the same as "no rows this period" rather than raising.
        # Raising here would skip processed_files.append() for this file
        # (see the FK_ADS_ORDERS call site), which means it never gets marked
        # processed and gets silently re-downloaded and re-failed every run.
        print(f"  FK Ads Orders: {path.name} is empty, treating as 0 rows")
        return []
    df.columns = [str(c).strip() for c in df.columns]

    camp_id    = next((c for c in df.columns if 'campaign id' in c.lower()), None)
    date_col   = next((c for c in df.columns if c.lower() == 'date'), None)
    order_col  = next((c for c in df.columns if 'order_id' in c.lower()), None)
    adv_sku    = next((c for c in df.columns if 'advertised fsn' in c.lower()), None)
    pur_sku    = next((c for c in df.columns if 'purchased fsn' in c.lower()), None)
    name_col   = next((c for c in df.columns if 'product name' in c.lower()), None)
    rev_col    = next((c for c in df.columns if 'total revenue' in c.lower()), None)
    du_col     = next((c for c in df.columns if 'direct units' in c.lower()), None)
    iu_col     = next((c for c in df.columns if 'indirect units' in c.lower()), None)

    if not camp_id or not order_col:
        print(f"  FK Ads Orders: required columns not found, skipping")
        return []

    rows = []
    for _, row in df.iterrows():
        cid      = str(row.get(camp_id,   '')).strip()
        order_id = str(row.get(order_col, '')).strip()
        if not cid or cid.lower() in ('nan', '') or not order_id or order_id.lower() in ('nan', ''):
            continue
        dt = str(pd.to_datetime(row.get(date_col, ''), errors='coerce').date()) \
             if date_col else _fk_ads_date_from_header(path)
        if dt == 'NaT' or not dt:
            dt = _fk_ads_date_from_header(path)
        adv_raw = str(row.get(adv_sku, '')).strip() if adv_sku else ''
        pur_raw = str(row.get(pur_sku, '')).strip() if pur_sku else ''
        du  = int(float(row.get(du_col, 0) or 0)) if du_col else 0
        iu  = int(float(row.get(iu_col, 0) or 0)) if iu_col else 0
        rows.append({
            'date':             dt,
            'campaign_id':      cid,
            'order_id':         order_id,
            'advertised_sku':   adv_raw,
            'purchased_sku':    pur_raw,
            'product_name':     str(row.get(name_col, '')).strip() if name_col else '',
            'revenue':          round(float(row.get(rev_col, 0) or 0), 2) if rev_col else 0.0,
            'units_direct':     du,
            'units_indirect':   iu,
        })

    print(f"  FK Ads Orders: {len(rows)} order rows")
    return rows


_FK_ADS_SCHEMAS = {
    'fk_ads_daily':      ['date', 'campaign_id', 'campaign_name',
                          'ad_spend', 'revenue', 'views', 'clicks', 'conversions', 'roas'],
    'fk_ads_sku':        ['date', 'campaign_id', 'campaign_name', 'sku_id', 'sku_name',
                          'views', 'clicks', 'units_sold', 'revenue', 'ad_spend', 'roas'],
    'fk_ads_kw':         ['date', 'campaign_id', 'campaign_name',
                          'keyword', 'match_type', 'views', 'clicks', 'spend'],
    'fk_ads_placements': ['date', 'campaign_id', 'campaign_name', 'placement_type',
                          'views', 'clicks', 'ad_spend', 'units_sold', 'revenue', 'roas'],
    'fk_ads_overall':    ['date', 'campaign_id', 'sku_id', 'sku_name', 'listing_id',
                          'cpc', 'views', 'clicks', 'units_direct', 'units_indirect',
                          'revenue_direct', 'revenue_indirect', 'ad_spend', 'revenue', 'roas'],
    'fk_ads_search':     ['date', 'campaign_id', 'campaign_name',
                          'query', 'views', 'clicks', 'spend', 'units_sold', 'revenue'],
    'fk_ads_order_items':['date', 'campaign_id', 'order_id',
                          'advertised_sku', 'purchased_sku', 'product_name',
                          'revenue', 'units_direct', 'units_indirect'],
}


def load_fk_ads_db(path):
    """Load rumee_db_fk_ads.csv into {table_name: [rows]}."""
    result = {t: [] for t in _FK_ADS_SCHEMAS}
    raw = load_db(path)
    for t in _FK_ADS_SCHEMAS:
        result[t] = raw.get(t, [])
    return result


def save_fk_ads_csv(tables, path):
    """Write rumee_db_fk_ads.csv. tables = {table_name: [rows]}."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for tname, cols in _FK_ADS_SCHEMAS.items():
            rows = tables.get(tname, [])
            w.writerow(['__table__'] + cols)
            for rec in rows:
                w.writerow([tname] + [rec.get(c, '') for c in cols])
    counts = {t: len(tables.get(t, [])) for t in _FK_ADS_SCHEMAS}
    print(f"  Saved rumee_db_fk_ads.csv:   "
          f"{counts['fk_ads_daily']} daily, {counts['fk_ads_sku']} SKU, "
          f"{counts['fk_ads_kw']} kw, {counts['fk_ads_placements']} placements, "
          f"{counts['fk_ads_overall']} overall, {counts['fk_ads_search']} search, "
          f"{counts['fk_ads_order_items']} ad-orders")


# ─── Meesho Ads DB (campaign / catalog / master) ─────────────────────────────

_ME_ADS_SCHEMAS = {
    'me_ads_daily':   ['date', 'campaign_id', 'campaign_name', 'status', 'budget',
                       'spend', 'revenue', 'orders', 'views', 'clicks', 'roi', 'cpo'],
    'me_ads_catalog': ['date', 'campaign_id', 'campaign_name', 'catalog_id', 'catalog_name',
                       'spend', 'revenue', 'orders', 'views', 'clicks', 'cpc'],
    'me_ads_master':  ['campaign_id', 'campaign_name', 'status', 'budget',
                       'total_spend', 'total_revenue', 'total_orders',
                       'total_views', 'total_clicks', 'roi'],
}


def load_me_ads_db(path):
    """Load rumee_db_me_ads.csv into {table_name: [rows]}."""
    result = {t: [] for t in _ME_ADS_SCHEMAS}
    raw = load_db(path)
    for t in _ME_ADS_SCHEMAS:
        result[t] = raw.get(t, [])
    return result


def save_me_ads_csv(tables, path):
    """Write rumee_db_me_ads.csv. tables = {table_name: [rows]}."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        for tname, cols in _ME_ADS_SCHEMAS.items():
            rows = tables.get(tname, [])
            w.writerow(['__table__'] + cols)
            for rec in rows:
                w.writerow([tname] + [rec.get(c, '') for c in cols])
    counts = {t: len(tables.get(t, [])) for t in _ME_ADS_SCHEMAS}
    print(f"  Saved rumee_db_me_ads.csv:   "
          f"{counts['me_ads_daily']} daily, {counts['me_ads_catalog']} catalog, "
          f"{counts['me_ads_master']} master")


# ─── FK Orders (Fulfilment daily / SKU) ──────────────────────────────────────

def process_fk_orders(path, last_date_str):
    """
    Process Flipkart Fulfilment Orders report (XLSX).
    Sheet 'Orders', single-row header.
    Columns used: order_date, order_id, sku, quantity.
    SKU values arrive triple-quoted with 'SKU:' prefix — stripped here.
    Returns:
        daily_rows: [{date, orders, quantity}]
        sku_rows:   [{date, sku, orders, quantity}]
        order_rows: [{order_id, order_date, sku, qty}] — establishes order
            identity from THIS file (active.md item #67, 2026-07-19). Until
            now, fk_order_sku_index (the per-order status/Ledger registry)
            was seeded ONLY from the FK_PAYMENTS file, so an order with no
            Payments match yet (Payments files land far less often than
            this Orders/Fulfilment file) never appeared in the registry or
            the Ledger at all, even though it's a real, placed order. This
            makes the Orders file the identity source — Payments becomes a
            pure override, matching how Meesho already works.
        new_last:   str — max order_date seen
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()

    xl = pd.ExcelFile(path)
    orders_sheet = next((s for s in xl.sheet_names if 'order' in s.lower()), xl.sheet_names[0])
    df = xl.parse(orders_sheet)

    if df.empty:
        return [], [], [], last_date_str

    df.columns = [str(c).lower().strip() for c in df.columns]

    dates = pd.to_datetime(df.get('order_date', pd.Series(dtype='object')), errors='coerce').dt.date
    valid = dates.notna()
    df2 = df[valid].copy()
    df2['_dt'] = dates[valid].values

    df_new = df2[df2['_dt'] > last_date]
    if df_new.empty:
        print(f"  FK Orders: 0 new rows (last={last_date_str})")
        return [], [], [], last_date_str

    def _clean_sku(s):
        s = str(s).strip('"').strip()
        if s.upper().startswith('SKU:'):
            s = s[4:].strip()
        return s

    df_new = df_new.copy()
    df_new['_sku'] = df_new['sku'].apply(_clean_sku) if 'sku' in df_new.columns else ''
    df_new['_qty'] = pd.to_numeric(df_new.get('quantity', 1), errors='coerce').fillna(1).astype(int)

    new_last = df_new['_dt'].max()

    daily_rows = []
    for dt, grp in df_new.groupby('_dt'):
        daily_rows.append({
            'date':     dt.isoformat(),
            'orders':   int(grp['order_id'].nunique()) if 'order_id' in grp.columns else len(grp),
            'quantity': int(grp['_qty'].sum()),
        })

    sku_rows = []
    for (dt, sku), grp in df_new.groupby(['_dt', '_sku']):
        sku_rows.append({
            'date':     dt.isoformat(),
            'sku':      sku,
            'orders':   int(grp['order_id'].nunique()) if 'order_id' in grp.columns else len(grp),
            'quantity': int(grp['_qty'].sum()),
        })

    # Per-order identity rows (active.md item #67) -- one per order_id, last
    # line for a given order_id wins if the file has multiple lines per
    # order (matches the dict-overwrite convention used everywhere else this
    # registry is built/merged).
    order_rows = []
    if 'order_id' in df_new.columns:
        for _, row in df_new.iterrows():
            oid = str(row.get('order_id', '') or '').strip()
            if not oid:
                continue
            order_rows.append({
                'order_id':   oid,
                'order_date': row['_dt'].isoformat(),
                'sku':        row['_sku'],
                'qty':        int(row['_qty']),
            })

    print(f"  FK Orders: {len(df_new)} rows, {len(daily_rows)} daily, "
          f"{len(sku_rows)} SKU rows, {len(order_rows)} order rows "
          f"({df_new['_dt'].min()} to {new_last})")

    return daily_rows, sku_rows, order_rows, new_last.isoformat()


# ─── Flipkart Returns (reason code aggregation) ──────────────────────────────

def process_fk_returns(path, last_date_str):
    """
    Process Flipkart Fulfilment Returns report (CSV/XLSX).

    Bucketed by COMPLETED DATE (when the return closed) — the file is a daily
    snapshot of returns completed that day, so Completed Date is monotonic across
    files and matches the orders timeline. Return Type splits courier_return
    (RTO / undelivered) vs customer_return (true post-delivery return).

    Dedup is by Return ID within the file (each return row has a unique id).

    Returns:
        daily_rows: [{date, returns, courier_returns, customer_returns, quantity}, ...]
        sku_rows:   [{date, sku, returns, courier_returns, customer_returns, quantity}, ...]
        reasons:    {reason_str: count}  — keys are "Reason > Sub-Reason" or just "Reason"
        new_last:   str — max completed date seen (ISO), or last_date_str if no new rows
        order_awb_index: {order_id: tracking_id} — from the report's own "Order ID" and
            "Tracking ID" columns (confirmed present 2026-07-14 against a real Drive
            file), used by the Orders Ledger to resolve a Return Receipts scan that only
            captured the AWB (not the Order ID) back to the real order_id.
        order_sku_index: {order_id: sku} — the report's own "SKU" column,
            keyed by Order ID. Added 2026-07-21 (dashboard memory active.md
            item #72) so the Returns Scanner's SKU lookup can prefer the
            RETURN's own reported SKU over the order-placement-time guess,
            once this specific return has actually synced.
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()

    if str(path).lower().endswith('.csv'):
        df = pd.read_csv(path, dtype=str, on_bad_lines='skip')
    else:
        xl = pd.ExcelFile(path)
        sheet = next((s for s in xl.sheet_names if 'return' in s.lower()), xl.sheet_names[0])
        df = xl.parse(sheet)

    if df.empty:
        return [], [], {}, last_date_str, {}, {}

    df.columns = [str(c).lower().strip() for c in df.columns]

    def _find(*needles):
        return next((c for c in df.columns if all(n in c for n in needles)), None)

    date_col   = _find('completed', 'date')          # bucket by Completed Date
    reason_col = next((c for c in df.columns if 'return reason' in c), None)
    sub_col    = next((c for c in df.columns if 'sub' in c and 'reason' in c), None)
    type_col   = _find('return', 'type')
    rid_col    = _find('return id')
    sku_col    = next((c for c in df.columns if c == 'sku'), None) or _find('sku')
    qty_col    = _find('quantity')
    order_col  = next((c for c in df.columns if c == 'order id'), None)
    awb_col    = next((c for c in df.columns if 'tracking id' in c), None)

    if not date_col or not reason_col:
        print(f"  FK Returns: required columns not found in {path.name} "
              f"(completed_date={date_col}, reason={reason_col})")
        return [], [], {}, last_date_str, {}, {}

    dates = pd.to_datetime(df[date_col], errors='coerce').dt.date
    valid = dates.notna()
    df2 = df[valid].copy()
    df2['_dt'] = dates[valid].values
    df_new = df2[df2['_dt'] > last_date]
    if rid_col:
        df_new = df_new.drop_duplicates(subset=[rid_col])

    if df_new.empty:
        print(f"  FK Returns: 0 new rows (last completed={last_date_str})")
        return [], [], {}, last_date_str, {}, {}

    daily   = {}   # date -> {returns, courier_returns, customer_returns, quantity}
    sku_agg = {}   # (date, sku) -> same
    reasons = {}
    order_awb_index = {}
    order_sku_index = {}
    for _, row in df_new.iterrows():
        cd = row['_dt'].isoformat()
        rtype = str(row.get(type_col, '') or '').strip().lower() if type_col else ''
        is_courier = 'courier' in rtype
        try:
            qty = int(float(row.get(qty_col, 1) or 1)) if qty_col else 1
        except (ValueError, TypeError):
            qty = 1

        d = daily.setdefault(cd, {'returns': 0, 'courier_returns': 0,
                                  'customer_returns': 0, 'quantity': 0})
        d['returns'] += 1
        d['quantity'] += qty
        d['courier_returns' if is_courier else 'customer_returns'] += 1

        sname = str(row.get(sku_col, '') or '').strip() if sku_col else ''
        s = sku_agg.setdefault((cd, sname), {'returns': 0, 'courier_returns': 0,
                                             'customer_returns': 0, 'quantity': 0})
        s['returns'] += 1
        s['quantity'] += qty
        s['courier_returns' if is_courier else 'customer_returns'] += 1

        r = str(row.get(reason_col, '') or '').strip()
        sub = str(row.get(sub_col, '') or '').strip() if sub_col else ''
        key = f"{r} > {sub}" if sub and sub.lower() not in ('nan', 'none', '') else r
        if key:
            reasons[key] = reasons.get(key, 0) + 1

        if order_col and awb_col:
            oid = str(row.get(order_col, '') or '').strip()
            awb = str(row.get(awb_col, '') or '').strip()
            if oid and oid.lower() != 'nan' and awb and awb.lower() != 'nan':
                order_awb_index[oid] = awb

        if order_col and sku_col:
            oid2 = str(row.get(order_col, '') or '').strip()
            if oid2 and oid2.lower() != 'nan' and sname:
                order_sku_index[oid2] = sname

    daily_rows = [dict(date=k, **v) for k, v in daily.items()]
    sku_rows   = [dict(date=k[0], sku=k[1], **v) for k, v in sku_agg.items()]
    new_last = df_new['_dt'].max()
    print(f"  FK Returns: {len(df_new)} new rows, {len(daily_rows)} days, "
          f"{len(reasons)} reason codes ({df_new['_dt'].min()} to {new_last})")
    return daily_rows, sku_rows, reasons, new_last.isoformat(), order_awb_index, order_sku_index


# ─── Flipkart Listings (OG vs Bahubali pricing pairs) ────────────────────────

def process_fk_listings(path, pm_overrides=None):
    """
    Read Flipkart Master Listing file (XLS/XLSX) and build fk_pairs table.
    Also builds a needs_review list for product_master (see below) —
    this is additive, the existing fk_pairs/fsn_map behavior is unchanged.

    Row 0 of the sheet is a descriptions row (not data) — skip it.
    Only DJ- SKUs are used for the OG/Bahubali pricing-pairs table below.
    Bahubali vs OG classification is based on whether the Product Title
    contains 'Bahubali' (case-insensitive).

    Returns: (pairs, fsn_map, needs_review, fk_variation_entries)
        fk_variation_entries: {label_folder: {design, variation_type, platform, listings:[...]}}
            — real product_master listings keyed by the label folder from
            pm_overrides (Option A single source of truth, 2026-07-03).
        pairs: list of dicts matching fk_pairs schema:
            [{'base', 'og_name', 'og_mrp', 'og_selling', 'og_settlement',
              'bahu_name', 'bahu_mrp', 'bahu_selling', 'bahu_settlement',
              'status', 'verdict'}, ...]
            status: 'pair' (both OG and Bahubali found) | 'solo' (only one variant)
        fsn_map: {seller_sku_id: fsn}
        needs_review: list of {platform, catalog_id, raw_sku, product_name}
            for rows whose FSN has no pm_overrides entry (label single source of
            truth — no slug map fallback). platform is 'flipkart' or 'shopsy' —
            Shopsy detected via Sub-category starting with 'shopsy_'
            (platform-assigned signal, not the user-typed SKU text).
    """
    import re
    pm_overrides = pm_overrides or {}
    needs_review = []
    fk_variation_entries = {}   # {label_folder: {design, variation_type, platform, listings:[...]}}

    try:
        xl = pd.ExcelFile(path)
        df = xl.parse(xl.sheet_names[0])   # header=0 → row 0 = column names
        df = df.iloc[1:].reset_index(drop=True)  # drop description row
    except Exception as e:
        print(f"  FK Listings: read error — {e}")
        _run_warnings.append({'file': str(path), 'type': 'FK', 'reason': f"FK Listings read failed: {e}",
                               'impact': "this Listings file was skipped this run — OG/Bahubali pricing pairs and product_master enrichment won't reflect it until a future run recovers it"})
        return [], {}, [], {}

    # Identify columns (by name from header row)
    title_col = 'Product Title'
    sku_col   = 'Seller SKU Id'
    mrp_col   = 'MRP'
    sett_col  = 'Bank Settlement'
    sell_col  = 'Your Selling Price'
    subcat_col = 'Sub-category' if 'Sub-category' in df.columns else None
    url_col   = next((c for c in ['Link for Buyer Portal', 'Product URL', 'Buyer Portal Link',
                                   'Listing URL', 'URL'] if c in df.columns), None)
    fsn_col   = next((c for c in ['Flipkart Serial Number', 'FSN'] if c in df.columns), None)

    # Build FSN map for ALL SKUs (sku_id → FSN) before filtering to DJ- only
    fsn_map = {}
    if fsn_col:
        for _, row in df.iterrows():
            sku_raw = str(row.get(sku_col, '')).strip()
            fsn_raw = str(row.get(fsn_col, '')).strip()
            if sku_raw and fsn_raw and fsn_raw.lower() not in ('nan', ''):
                fsn_map[sku_raw] = fsn_raw
        print(f"  FK Listings: {len(fsn_map)} SKUs with FSN")

    # ── product_master resolution pass — LABEL-BASED (Option A, 2026-07-03) ──
    # Builds REAL product_master listings for FK/Shopsy keyed by the label folder
    # from pm_overrides (single source of truth) — previously this pass only
    # produced needs_review and no listings (the missing FK build). The slug map
    # fk_sku_id_for_pm is NOT used here (sales joins only). Unknown FSN ->
    # needs_review, never auto-created.
    stock_col_fk = next((c for c in ['System Stock count', 'Your Stock Count']
                         if c in df.columns), None)
    if fsn_col:
        for _, row in df.iterrows():
            raw_sku = str(row.get(sku_col, '')).strip()
            catalog_id = str(row.get(fsn_col, '')).strip()
            if not raw_sku or not catalog_id or catalog_id.lower() == 'nan':
                continue

            subcat = str(row.get(subcat_col, '')).strip() if subcat_col else ''
            if subcat.lower() == 'nan':
                subcat = ''
            if subcat.startswith('shopsy_'):
                platform = 'shopsy'
            elif not subcat and 'shopsy' in raw_sku.lower():
                # No reliable platform-assigned signal — route to needs_review
                # rather than silently defaulting to flipkart (owner-confirmed
                # 2026-07-01: SKU text alone is unreliable, user-typed).
                pname = str(row.get(title_col, '')).strip() if title_col in df.columns else ''
                needs_review.append({
                    'platform': 'flipkart', 'catalog_id': catalog_id,
                    'raw_sku': raw_sku, 'product_name': pname,
                    'reason': 'SKU text mentions "shopsy" but no Sub-category was set — could not confirm this is a real Shopsy listing',
                })
                continue
            else:
                platform = 'flipkart'

            ov = pm_overrides.get(f'{platform}_{catalog_id}')
            if ov and ov.get('target_sku_id') == '__REJECTED__':
                continue  # owner discarded this via the dashboard — never re-surface it
            if not (ov and ov.get('target_sku_id')):
                pname = str(row.get(title_col, '')).strip() if title_col in df.columns else ''
                needs_review.append({
                    'platform': platform, 'catalog_id': catalog_id,
                    'raw_sku': raw_sku, 'product_name': pname,
                    'reason': f'Seller SKU "{raw_sku}" is not in your saved SKU list yet',
                })
                continue
            sid    = ov['target_sku_id']                       # label folder
            vtype  = ov.get('target_variation_type') or 'Base'
            design = ov.get('target_design') or sid

            stock = 0
            if stock_col_fk:
                sv = row.get(stock_col_fk, None)
                try:
                    if sv is not None and not pd.isna(sv):
                        stock = int(float(sv))
                except (ValueError, TypeError):
                    pass

            lstatus = str(row.get('Listing Status', '')).strip().upper()
            suggested_inactive = bool(lstatus) and lstatus != 'ACTIVE'
            buyer_url = str(row.get(url_col, '')).strip() if url_col else ''
            if buyer_url.lower() == 'nan':
                buyer_url = ''

            if sid not in fk_variation_entries:
                fk_variation_entries[sid] = {
                    'design': design, 'variation_type': vtype,
                    'platform': platform, 'listings': [],
                }
            fk_variation_entries[sid]['listings'].append({
                'sku_id':             raw_sku,
                'catalog_id':         catalog_id,
                'fsn':                catalog_id,
                'stock':              stock,
                'buyer_url':          buyer_url,
                'low_stock_alert':    stock == 0,
                'suggested_inactive': suggested_inactive,
                'platform':           platform,
            })
        print(f"  FK Listings: {len(fk_variation_entries)} variations, "
              f"{sum(len(v['listings']) for v in fk_variation_entries.values())} listings, "
              f"{len(needs_review)} unmapped -> needs_review")

    # Filter to DJ- SKUs only (unchanged — pricing-pairs table is DJ-series only)
    dj = df[df[sku_col].astype(str).str.contains('DJ-', na=False)].copy()
    if dj.empty:
        print("  FK Listings: no DJ- SKUs found")
        return [], fsn_map, needs_review, fk_variation_entries

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
                p['bahu_url']        = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else ''
        else:
            # If multiple OG variants for same base, keep first
            if 'og_name' not in p:
                p['og_name']       = sku_str
                p['og_mrp']        = mrp
                p['og_selling']    = sell
                p['og_settlement'] = sett
                p['og_url']        = str(row[url_col]) if url_col and pd.notna(row.get(url_col)) else ''

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
            'og_url':          p.get('og_url', ''),
            'bahu_name':       p.get('bahu_name', ''),
            'bahu_mrp':        p.get('bahu_mrp', 0),
            'bahu_selling':    p.get('bahu_selling', 0),
            'bahu_settlement': p.get('bahu_settlement', 0),
            'bahu_url':        p.get('bahu_url', ''),
            'status':          'pair' if (has_og and has_bahu) else 'solo',
            'verdict':         verdict,
        })

    pairs_count = sum(1 for r in result if r['status'] == 'pair')
    print(f"  FK Listings: {len(result)} base SKUs, {pairs_count} OG/Bahubali pairs")
    return result, fsn_map, needs_review, fk_variation_entries


# ─── Flipkart Views ───────────────────────────────────────────────────────────

def _csv_header_row(path):
    """Return the first row index that has the most comma-separated fields (= real header)."""
    max_fields, header_row = 0, 0
    try:
        with open(path, encoding='utf-8', errors='replace') as f:
            for i, line in enumerate(f):
                n = len(line.split(','))
                if n > max_fields:
                    max_fields, header_row = n, i
                if i >= 20:
                    break
    except Exception:
        pass
    return header_row


def _read_tabular(path, dtype=None):
    """Read CSV or XLSX transparently. Handles preamble rows by auto-detecting header."""
    ext = Path(path).suffix.lower()
    if ext in ('.xlsx', '.xls'):
        return pd.read_excel(path, dtype=dtype or {})
    try:
        return pd.read_csv(path, dtype=dtype or {}, encoding='utf-8', encoding_errors='replace')
    except pd.errors.ParserError:
        try:
            skip = _csv_header_row(path)
            return pd.read_csv(path, skiprows=range(skip), dtype=dtype or {},
                               encoding='utf-8', encoding_errors='replace',
                               engine='python', on_bad_lines='skip')
        except Exception:
            return pd.DataFrame()


def process_fk_views(path, last_date_str):
    """Returns skus: {sku_id: {views, clicks, sales, revenue, ctr}} and new_last_date."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = _read_tabular(path, dtype={'Impression Date': str})
    if 'Impression Date' not in df.columns:
        print(f"  FK Views: 'Impression Date' not found in {path.name} — skipping")
        return {}, last_date_str

    df['_dt'] = pd.to_datetime(df['Impression Date'], errors='coerce').dt.date
    df = df[df['_dt'].notna()]
    df_new = df[_dt_gt(df['_dt'], last_date)]
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

def _merge_pm_entries(dest, src):
    """Merge product_master variation entries {label_folder: {..., listings}}
    from src into dest. Dedup key = product_id-or-catalog_id, matching the
    Firestore writer's _mkey — Meesho is per-product_id, so a plain catalog_id
    key would silently drop distinct products sharing one Meesho catalog."""
    def _k(l):
        return str(l.get('product_id') or l.get('catalog_id') or '')
    for sid, entry in src.items():
        if sid in dest:
            seen = {_k(l) for l in dest[sid]['listings']}
            for lst in entry['listings']:
                k = _k(lst)
                if k and k not in seen:
                    dest[sid]['listings'].append(lst)
                    seen.add(k)
        else:
            dest[sid] = entry


def process_catalog(path, pm_overrides=None):
    """
    Returns (variation_entries, variation_entries, needs_review) for Meesho catalog.
    First value is used as a truthy check only.

    variation_entries = {
        sku_id: {
            'design':         str,   # base design group label (e.g. "DJ-7")
            'variation_type': str,   # "og" | "bahubali" | "base"
            'platform':       'me',
            'listings': [
                { 'style_id', 'catalog_id', 'product_id', 'me_url', 'stock' }, ...
            ]
        }
    }

    needs_review: list of {platform:'meesho', catalog_id, raw_sku, product_name}
    for rows whose product_id has no pm_overrides entry (label single source of
    truth — no slug map fallback).
    Root-cause fix: unmapped SKUs are NEVER auto-slugified into a new
    product_master doc — they go to needs_review for manual assignment.
    """
    pm_overrides = pm_overrides or {}
    needs_review = []
    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        print(f"  Catalog: could not open {path.name} — {e}. Skipping.")
        _run_warnings.append({'file': str(path), 'type': 'CATALOG', 'reason': f"Catalog file could not be opened: {e}",
                               'impact': "this catalog file was skipped this run — Meesho style-to-SKU mapping won't reflect it until a future run recovers it"})
        return {}, {}, []
    df = xl.parse(xl.sheet_names[0])
    xl.close()
    df.columns = [str(c).strip() for c in df.columns]

    if 'Row identifier' in str(df.iloc[0, 0]):
        df = df.iloc[1:].reset_index(drop=True)

    style_col        = next((c for c in df.columns if 'STYLE ID'     in c.upper()), None)
    stock_col        = next((c for c in df.columns if 'SYSTEM STOCK' in c.upper()), None)
    catalog_col      = next((c for c in df.columns if 'CATALOG ID'   in c.upper()), None)
    catalog_name_col = next((c for c in df.columns if 'CATALOG NAME' in c.upper()), None)
    product_name_col = next((c for c in df.columns if c.upper().strip() == 'PRODUCT NAME'), None)
    product_id_col   = next((c for c in df.columns if c.upper().strip() == 'PRODUCT ID'), None)

    if not style_col:
        print(f"  Catalog: Could not find STYLE ID column. Found: {list(df.columns)}")
        return {}, {}, []

    variation_entries = {}   # {label_folder: {design, variation_type, platform, listings: [...]}}
    seen_product_ids  = set()  # Meesho product_master keyed by PRODUCT ID (per-product)
    skipped = 0

    for _, row in df.iterrows():
        # Blocklist check: store/seller names (e.g. "Meera Craft Store") show
        # up inside PRODUCT NAME text, not CATALOG NAME (which is a category
        # like "Fancy Paintings & Posters") — check both, bug found 2026-07-02
        # where CATALOG NAME-only checking silently let these rows through.
        blocked_hit = False
        if catalog_name_col:
            cname = str(row.get(catalog_name_col, '')).strip().lower()
            if any(blocked in cname for blocked in ME_CATALOG_BLOCKLIST):
                blocked_hit = True
        if not blocked_hit and product_name_col:
            pname_check = str(row.get(product_name_col, '')).strip().lower()
            if any(blocked in pname_check for blocked in ME_CATALOG_BLOCKLIST):
                blocked_hit = True
        # Permanent catalog-ID blocklist — survives any text/spelling change.
        if not blocked_hit and catalog_col:
            cid_check = str(row.get(catalog_col, '')).strip()
            if cid_check in ME_CATALOG_ID_BLOCKLIST:
                blocked_hit = True
        if blocked_hit:
            skipped += 1
            continue

        raw = str(row.get(style_col, '')).strip()
        if not raw or raw == 'nan':
            continue

        cat_raw = str(row.get(catalog_col, '')).strip() if catalog_col else ''
        if not cat_raw or cat_raw.lower() in ('nan', ''):
            continue

        # Meesho product_master is keyed by PRODUCT ID (unique per style), NOT
        # catalog_id — one Meesho catalog can hold several distinct products,
        # each its own listing/folder (owner decision 2026-07-03). product_id is
        # platform-assigned and reliable, same guarantee catalog_id gives FK/AZ.
        # The meesho catalog_id is still stored on the listing (display).
        product_id_str = ''
        if product_id_col:
            try:
                product_id_str = str(int(float(row.get(product_id_col))))
            except (ValueError, TypeError):
                product_id_str = ''
        if not product_id_str:
            continue
        if product_id_str in seen_product_ids:
            continue
        seen_product_ids.add(product_id_str)

        # product_master resolution — LABEL-BASED single source of truth
        # (Option A, 2026-07-03). pm_overrides (the unified label map, keyed by
        # meesho_{product_id}) decides design/variation/folder for EVERY listing.
        # slug map me_sku_id_for_pm is NOT used here (sales joins only). Unknown
        # product_id -> needs_review, never auto-created into a slug doc.
        ov = pm_overrides.get(f'meesho_{product_id_str}')
        if ov and ov.get('target_sku_id') == '__REJECTED__':
            continue  # owner discarded this via the dashboard — never re-surface it
        if not (ov and ov.get('target_sku_id')):
            pname = str(row.get(product_name_col, '')).strip() if product_name_col else ''
            needs_review.append({
                'platform': 'meesho', 'catalog_id': product_id_str,
                'raw_sku': raw, 'product_name': pname,
                'reason': f'Style ID "{raw}" is not in your saved SKU list yet',
            })
            continue
        sid            = ov['target_sku_id']                       # label folder
        vtype_override = ov.get('target_variation_type') or 'Base'
        design_override = ov.get('target_design') or sid

        stock = 0
        if stock_col:
            cnt = row.get(stock_col, None)
            try:
                if cnt is not None and not pd.isna(cnt):
                    stock = int(float(cnt))
            except (ValueError, TypeError):
                pass

        # me_url = deterministic base36 of product_id (already validated above)
        me_url = ''
        try:
            digits = '0123456789abcdefghijklmnopqrstuvwxyz'
            b36, n = '', int(product_id_str)
            while n:
                b36 = digits[n % 36] + b36
                n //= 36
            me_url = f'https://www.meesho.com/product/p/{b36}' if b36 else ''
        except (ValueError, TypeError):
            pass

        if sid not in variation_entries:
            variation_entries[sid] = {
                'design':         design_override,
                'variation_type': vtype_override,
                'platform':       'me',
                'listings':       [],
            }
        variation_entries[sid]['listings'].append({
            'style_id':   raw,
            'catalog_id': cat_raw,        # meesho catalog id (display)
            'product_id': product_id_str, # unique per-product merge key
            'me_url':     me_url,
            'stock':      stock,
        })

    total_listings = sum(len(v['listings']) for v in variation_entries.values())
    print(f"  Catalog: {len(variation_entries)} variations, {total_listings} listings, "
          f"{skipped} rows skipped (blocked catalog name), {len(needs_review)} unmapped -> needs_review")
    return variation_entries, variation_entries, needs_review

# ─── Amazon Catalog (product_master) ─────────────────────────────────────────

def process_az_catalog_for_pm(pm_overrides=None):
    """
    Reads the latest rumee_az_catalog/{YYYY_MM} Firestore doc and resolves
    listings for product_master, same pattern as process_catalog/process_fk_listings.
    Reads the latest available month dynamically — never hardcodes a month.

    Returns: (listings_by_sku, needs_review)
        listings_by_sku: {sku_id: {design, variation_type, listings: [...]}}
        needs_review: list of {platform:'amazon', catalog_id, raw_sku, product_name}
    """
    pm_overrides = pm_overrides or {}
    listings_by_sku = {}
    needs_review = []
    try:
        from firestore_connector import get_db
        db = get_db()
        docs = list(db.collection('rumee_az_catalog').stream())
        if not docs:
            print("  AZ Catalog: no rumee_az_catalog docs found")
            return {}, []
        # Doc ids are YYYY_MM — pick the lexicographically latest, never hardcode
        latest = max(docs, key=lambda d: d.id)
        data = latest.to_dict() or {}
        rows = data.get('rows', [])
        print(f"  AZ Catalog: using {latest.id} ({len(rows)} rows)")
    except Exception as e:
        print(f"  AZ Catalog: read error — {e}")
        _run_warnings.append({'file': 'az_catalog_firestore', 'type': 'AMAZON', 'reason': f"AZ Catalog read failed: {e}",
                               'impact': "Amazon catalog data wasn't refreshed this run — Products tab may show stale Amazon listings until a future run recovers"})
        return {}, []

    for row in rows:
        raw_sku = str(row.get('seller-sku', '')).strip()
        asin1   = str(row.get('asin1', '')).strip()
        listing_id = str(row.get('listing-id', '')).strip()
        catalog_id = asin1 if asin1 and asin1.lower() != 'nan' else listing_id
        if not raw_sku or not catalog_id or catalog_id.lower() == 'nan':
            continue

        # LABEL-BASED single source of truth (Option A, 2026-07-03) — pm_overrides
        # keyed by catalog_id decides design/variation/folder. az_sku_id_for_pm
        # slug map not used for product_master. Unknown catalog_id -> needs_review.
        ov = pm_overrides.get(f'amazon_{catalog_id}')
        if ov and ov.get('target_sku_id') == '__REJECTED__':
            continue  # owner discarded this via the dashboard — never re-surface it
        if not (ov and ov.get('target_sku_id')):
            needs_review.append({
                'platform': 'amazon', 'catalog_id': catalog_id,
                'raw_sku': raw_sku, 'product_name': str(row.get('item-name', '')).strip(),
                'reason': f'Seller SKU "{raw_sku}" is not in your saved SKU list yet',
            })
            continue
        sid    = ov['target_sku_id']                        # label folder
        vtype  = ov.get('target_variation_type') or 'Base'
        design = ov.get('target_design') or sid

        try:
            stock = int(float(row.get('quantity', 0) or 0))
        except (ValueError, TypeError):
            stock = 0

        status_raw = str(row.get('status', '')).strip().lower()
        # Actual Amazon status values not fully confirmed in this codebase yet —
        # default to False (no suggested_inactive) rather than guess wrong;
        # only flag when status is explicitly a known non-live value.
        suggested_inactive = status_raw in ('inactive', 'suppressed', 'incomplete')

        if sid not in listings_by_sku:
            listings_by_sku[sid] = {
                'design': design,
                'variation_type': vtype,
                'listings': [],
            }
        listings_by_sku[sid]['listings'].append({
            'sku_id':      raw_sku,
            'catalog_id':  catalog_id,
            'stock':       stock,
            'buyer_url':   f'https://www.amazon.in/dp/{asin1}' if asin1 and asin1.lower() != 'nan' else '',
            'low_stock_alert':    stock == 0,
            'suggested_inactive': suggested_inactive,
        })

    print(f"  AZ Catalog: {len(listings_by_sku)} variations resolved, {len(needs_review)} unmapped -> needs_review")
    return listings_by_sku, needs_review

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


# ─── Orders Ledger ───────────────────────────────────────────────────────────

def build_fk_ledger_rows(pay_order_rows, fk_claims_list,
                         return_receipts, packaging_config, fk_ads_sku_data,
                         fk_order_awb_index=None):
    """
    Builds final ledger rows for FK orders by enriching pay_order_rows
    (active.md item #67, 2026-07-19: now the Orders-file-primary +
    Payments-override merged registry, not raw Payments rows -- qty comes
    directly from each row, no separate (date,sku) lookup needed anymore)
    with:
      - return receipt condition (earring/box/chain) keyed by order_id
      - claim status/recovered from fk_claims
      - packaging_cost from packaging_config (flat, applied to every order --
        unchanged, Jaiswal did not ask to change this figure)
      - ad_spend_apportioned from fk_ads_sku_data
      - return_loss_value (COGS × 1 if earring Damaged)
      - packaging_loss (dashboard memory active.md #46, 2026-07-12): REAL
        condition-based loss, not a flat guess -- Label/Branded Poly/Brand
        Card/Transparent Poly always lost on any return; Keeper 33 Box (or
        Corrugated Box)/Rumee Sticker lost together only if box condition =
        Damaged. Uses packaging_config's always_lost_cost/box_sticker_cost
        (from firestore_connector.fetch_packaging_costs(), real Materials
        data), not packaging_cost_per_order.
      - chain_loss (NEW, own column, not folded into packaging_loss per
        Jaiswal's explicit instruction): chain_cost only if chain condition
        = Damaged. Blank/Intact/not-applicable (e.g. an OG order with no
        chain) all resolve to 0 -- the "treat historical returns as always
        lost" rule Jaiswal gave applies only to the separate, not-yet-built
        backfill (active.md #46), not to this go-forward calculation.
      - net_pl

    packaging_config: dict with 'packaging_cost_per_order' (Rs.),
      'bubble_wrap_cutoff' (YYYY-MM-DD), and the three real cost components
      from fetch_packaging_costs() -- 'always_lost_cost', 'box_sticker_cost',
      'chain_cost'.
    fk_ads_sku_data:  {sku: {date: {ad_spend, orders}}} for apportionment
    fk_order_awb_index: {order_id: tracking_id} from process_fk_returns -- used to
        resolve a Return Receipts row that only recorded the AWB (Order ID wasn't
        captured during scanning) back to this order's receipt.
    """
    fk_order_awb_index = fk_order_awb_index or {}
    if not pay_order_rows:
        return []

    # Build claims lookup: order_id -> {claim_id, claim_status, claim_recovered}
    claims_index = {}
    for c in fk_claims_list:
        oid = str(c.get('order_id', '') or c.get('order_item_id', '')).strip()
        if oid:
            claims_index[oid] = {
                'claim_id':        c.get('claim_id', ''),
                'claim_status':    c.get('status', ''),
                'claim_recovered': float(c.get('approved_amount', 0) or 0),
            }

    pkg_cost        = float(packaging_config.get('packaging_cost_per_order', 12.0))
    bubble_cutoff   = packaging_config.get('bubble_wrap_cutoff', '2026-05-01')
    always_lost_cost = float(packaging_config.get('always_lost_cost', 0.0))
    box_sticker_cost = float(packaging_config.get('box_sticker_cost', 0.0))
    chain_cost       = float(packaging_config.get('chain_cost', 0.0))

    ledger_rows = []
    for row in pay_order_rows:
        oid   = row.get('order_id', '')
        dt    = row.get('order_date', '')
        sku   = row.get('sku', '')
        cogs  = float(row.get('cogs', 0) or 0)

        # qty comes directly from the merged registry row now (sourced from
        # the Orders file itself), not a separate (date,sku) lookup.
        qty = row.get('qty', 1)

        # return receipt condition -- keyed by order_id first; if the receipt
        # was scanned with only the AWB (order_id never captured), fall back
        # to the AWB this order's own FK_RETURNS row reports.
        receipt = return_receipts.get(oid) or return_receipts.get(fk_order_awb_index.get(oid, '')) or {}
        earring_cond = receipt.get('earring_condition', '')
        box_cond     = receipt.get('box_condition', '')
        chain_cond   = receipt.get('chain_condition', '')

        # claim info
        claim = claims_index.get(oid, {'claim_id': '', 'claim_status': 'not_raised', 'claim_recovered': 0.0})

        # packaging cost (add bubble wrap cost if order before cutoff) --
        # unchanged, this is the flat per-order figure applied to every
        # order regardless of return status, not the return-loss figures below.
        eff_pkg_cost = pkg_cost
        if dt < bubble_cutoff:
            eff_pkg_cost += float(packaging_config.get('bubble_wrap_cost', 2.0))

        # return losses (only if returned)
        status = row.get('status', '')
        is_returned = status in ('Returned-Customer', 'RTO')
        return_loss_value = cogs if (is_returned and earring_cond == 'Damaged') else 0.0
        packaging_loss = (
            always_lost_cost + (box_sticker_cost if box_cond == 'Damaged' else 0.0)
        ) if is_returned else 0.0
        chain_loss = chain_cost if (is_returned and chain_cond == 'Damaged') else 0.0

        # ad spend apportionment: ads[sku][date].ad_spend / ads[sku][date].orders
        sku_ads = fk_ads_sku_data.get(sku, {}).get(dt, {})
        ad_orders = int(sku_ads.get('orders', 0) or 1)
        ad_spend  = float(sku_ads.get('ad_spend', 0) or 0)
        ad_apport = round(ad_spend / ad_orders, 2) if ad_orders > 0 else 0.0

        sett    = float(row.get('settlement', 0) or 0)
        net_pl  = round(
            sett - cogs - eff_pkg_cost - ad_apport
            - return_loss_value - packaging_loss - chain_loss
            + claim['claim_recovered'],
            2
        )

        # Visibility columns (Jaiswal, 2026-07-14): matched_order_id shows the
        # order_id whenever a Return Receipt was found for this order -- via
        # direct order_id lookup OR the AWB fallback above, doesn't matter
        # which -- and is left BLANK when no receipt was found at all, so a
        # blank cell on a returned order is an immediate visual flag of a
        # match failure. return_pl isolates the P&L impact of the return
        # itself (claim recovery minus the three return-loss figures), not
        # the whole order's net_pl.
        matched_order_id = oid if receipt else ''
        return_pl = round(claim['claim_recovered'] - return_loss_value - packaging_loss - chain_loss, 2) if is_returned else 0.0

        ledger_rows.append({
            **row,
            'qty':               qty,
            'cogs':              round(cogs, 2),
            'packaging_cost':    round(eff_pkg_cost, 2),
            'ad_spend_apport':   ad_apport,
            'earring_condition': earring_cond,
            'box_condition':     box_cond,
            'chain_condition':   chain_cond,
            'return_loss_value': round(return_loss_value, 2),
            'packaging_loss':    round(packaging_loss, 2),
            'chain_loss':        round(chain_loss, 2),
            'claim_id':          claim['claim_id'],
            'claim_status':      claim['claim_status'],
            'claim_recovered':   round(claim['claim_recovered'], 2),
            'net_pl':            net_pl,
            'matched_order_id':  matched_order_id,
            'return_pl':         return_pl,
        })

    return ledger_rows


def derive_fk_sku_enrichment(ledger_rows):
    """
    Derives per-SKU enrichment columns from ledger rows for writing back to fk_skus:
      return_rate, rto_rate, net_pl (total), commission (total)
    Returns {sku_id: {return_rate, rto_rate, net_pl, commission}}
    Only includes final-status rows.
    """
    from sheets_connector import FINAL_STATUSES
    agg = {}
    for row in ledger_rows:
        if row.get('status') not in FINAL_STATUSES:
            continue
        sid = row.get('sku', '')
        if not sid:
            continue
        a = agg.setdefault(sid, {'orders': 0, 'returns': 0, 'rto': 0,
                                  'net_pl': 0.0, 'commission': 0.0})
        a['orders']     += 1
        if row.get('status') == 'Returned-Customer':
            a['returns'] += 1
        if row.get('status') == 'RTO':
            a['rto']     += 1
        a['net_pl']      = round(a['net_pl']     + float(row.get('net_pl', 0) or 0), 2)
        a['commission']  = round(a['commission'] + float(row.get('commission', 0) or 0), 2)

    result = {}
    for sid, a in agg.items():
        n = a['orders'] or 1
        result[sid] = {
            'return_rate': round((a['returns'] + a['rto']) / n * 100, 1),
            'rto_rate':    round(a['rto'] / n * 100, 1),
            'net_pl':      a['net_pl'],
            'commission':  a['commission'],
        }
    return result


# ─── Meesho Orders Ledger ─────────────────────────────────────────────────────

def build_me_ledger_rows(me_order_rows, me_return_reason_index,
                         me_claims_list, return_receipts, packaging_config,
                         me_suborder_awb_index=None):
    """
    Builds ledger rows for Meesho orders.
    me_order_rows:           per-order dicts (active.md item #67, 2026-07-19:
        the Orders-file-primary + Payments-override merged registry, not
        raw process_meesho_orders output directly -- same field shape,
        settlement/status may now be Payments-overridden)
    me_return_reason_index:  {suborder_id: return_reason_str} from process_meesho_returns
    me_claims_list:          db['me_claims']
    return_receipts:         from sheets_connector.fetch_return_receipts()
    packaging_config:        dict with packaging_cost_per_order, bubble_wrap_cost, bubble_wrap_cutoff
    me_suborder_awb_index:   {suborder_id: awb_number} from process_meesho_returns -- used to
        resolve a Return Receipts row that only recorded the AWB (Order ID/Suborder Number
        wasn't captured during scanning) back to this order's receipt.
    """
    me_suborder_awb_index = me_suborder_awb_index or {}
    if not me_order_rows:
        return []

    # Claims lookup: suborder_id → {claim_id, claim_status, claim_recovered}
    claims_index = {}
    for c in me_claims_list:
        sub_id = str(c.get('suborder_id', '') or c.get('order_id', '')).strip()
        if sub_id:
            claims_index[sub_id] = {
                'claim_id':        c.get('ticket_id', ''),
                'claim_status':    c.get('status', ''),
                'claim_recovered': float(c.get('amount_recovered', 0) or 0),
            }

    pkg_cost      = float(packaging_config.get('packaging_cost_per_order', 12.0))
    bubble_cutoff = packaging_config.get('bubble_wrap_cutoff', '2026-05-01')
    always_lost_cost = float(packaging_config.get('always_lost_cost', 0.0))
    box_sticker_cost = float(packaging_config.get('box_sticker_cost', 0.0))
    chain_cost       = float(packaging_config.get('chain_cost', 0.0))

    ledger_rows = []
    for row in me_order_rows:
        oid  = row.get('order_id', '')
        dt   = row.get('order_date', '')
        cogs = float(row.get('cogs', 0) or 0)

        # Return receipt condition (earring/box/chain) -- keyed by order_id
        # first; if the receipt was scanned with only the AWB (order_id/
        # suborder never captured), fall back to the AWB this order's own
        # ME_RETURNS row reports, so the receipt is still found.
        receipt = return_receipts.get(oid) or return_receipts.get(me_suborder_awb_index.get(oid, '')) or {}
        earring_cond = receipt.get('earring_condition', '')
        box_cond     = receipt.get('box_condition', '')
        chain_cond   = receipt.get('chain_condition', '')

        # Return reason from ME_RETURNS index
        return_reason = me_return_reason_index.get(oid, '')

        # Claim
        claim = claims_index.get(oid, {'claim_id': '', 'claim_status': 'not_raised', 'claim_recovered': 0.0})

        # Packaging cost -- unchanged, flat per-order figure applied to
        # every order regardless of return status.
        eff_pkg_cost = pkg_cost
        if dt < bubble_cutoff:
            eff_pkg_cost += float(packaging_config.get('bubble_wrap_cost', 2.0))

        # Return losses (dashboard memory active.md #46, 2026-07-12): real
        # condition-based packaging_loss + its own chain_loss, replacing the
        # old flat guess -- see build_fk_ledger_rows' docstring for the full
        # rule, identical here.
        status = row.get('status', '')
        is_returned = status in ('RTO', 'Returned-Customer')
        return_loss_value = cogs if (is_returned and earring_cond == 'Damaged') else 0.0
        packaging_loss = (
            always_lost_cost + (box_sticker_cost if box_cond == 'Damaged' else 0.0)
        ) if is_returned else 0.0
        chain_loss = chain_cost if (is_returned and chain_cond == 'Damaged') else 0.0

        sett   = float(row.get('settlement', 0) or 0)
        net_pl = round(
            sett - cogs - eff_pkg_cost
            - return_loss_value - packaging_loss - chain_loss
            + claim['claim_recovered'],
            2
        )

        # Visibility columns (Jaiswal, 2026-07-14) -- see build_fk_ledger_rows
        # for the full rationale, identical here.
        matched_order_id = oid if receipt else ''
        return_pl = round(claim['claim_recovered'] - return_loss_value - packaging_loss - chain_loss, 2) if is_returned else 0.0

        ledger_rows.append({
            **row,
            'cogs':              round(cogs, 2),
            'packaging_cost':    round(eff_pkg_cost, 2),
            'ad_spend_apport':   0.0,
            'return_reason':     return_reason,
            'earring_condition': earring_cond,
            'box_condition':     box_cond,
            'chain_condition':   chain_cond,
            'return_loss_value': round(return_loss_value, 2),
            'packaging_loss':    round(packaging_loss, 2),
            'chain_loss':        round(chain_loss, 2),
            'claim_id':          claim['claim_id'],
            'claim_status':      claim['claim_status'],
            'claim_recovered':   round(claim['claim_recovered'], 2),
            'net_pl':            net_pl,
            'matched_order_id':  matched_order_id,
            'return_pl':         return_pl,
        })

    return ledger_rows


def derive_me_sku_enrichment(ledger_rows):
    """
    Derives per-SKU return_rate, rto_rate, net_pl from Meesho ledger rows.
    Returns {sku_id: {return_rate, rto_rate, net_pl}}
    """
    from sheets_connector import FINAL_STATUSES
    agg = {}
    for row in ledger_rows:
        if row.get('status') not in FINAL_STATUSES:
            continue
        sid = row.get('sku', '')
        if not sid:
            continue
        a = agg.setdefault(sid, {'orders': 0, 'rto': 0, 'net_pl': 0.0})
        a['orders'] += 1
        if row.get('status') == 'RTO':
            a['rto'] += 1
        a['net_pl'] = round(a['net_pl'] + float(row.get('net_pl', 0) or 0), 2)

    result = {}
    for sid, a in agg.items():
        n = a['orders'] or 1
        result[sid] = {
            'return_rate': round(a['rto'] / n * 100, 1),
            'rto_rate':    round(a['rto'] / n * 100, 1),
            'net_pl':      a['net_pl'],
        }
    return result


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
        _run_warnings.append({'file': str(file_path), 'type': 'ME', 'reason': f"ME Claims read failed: {e}",
                               'impact': "this Meesho claims file was skipped this run — claims table won't reflect it until a future run recovers it"})
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
        _run_warnings.append({'file': str(file_path), 'type': 'FK', 'reason': f"FK Claims read failed: {e}",
                               'impact': "this Flipkart claims file was skipped this run — claims table won't reflect it until a future run recovers it"})
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
            _run_warnings.append({'file': str(file_path), 'type': 'FK', 'reason': f"FK Claims sheet '{sheet}' parse failed: {e}",
                                   'impact': f"the '{sheet}' sheet in this claims file was skipped — some claims from this file may be missing"})
            continue

        # Column detection
        claim_col    = next((c for c in df.columns if 'Claim ID' in c or c.lower() == 'claim id'), None)
        incident_col = next((c for c in df.columns if 'Incident' in c), None)
        order_col    = next((c for c in df.columns
                             if 'order id' in c.lower() and 'item' not in c.lower()), None)
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


def process_me_ads_summary(path, last_date_str):
    """Process ME_ADS_SUMMARY daily campaign CSV.
    Returns ({month: ad_spend}, [campaign_rows], new_last_str)."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"  ME Ads Summary: read error — {e}")
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"ME Ads Summary read failed: {e}",
                               'impact': "this file was skipped this run — Meesho ads summary won't reflect it until a future run recovers it"})
        return {}, [], last_date_str
    if df.empty:
        return {}, [], last_date_str
    df.columns = [c.strip() for c in df.columns]
    date_col    = next((c for c in df.columns if c.lower() == 'date'), None)
    spend_col   = next((c for c in df.columns if 'spend' in c.lower()), None)
    camp_id_col = next((c for c in df.columns if 'campaign id' in c.lower() or c.lower() == 'campaign_id'), None)
    camp_nm_col = next((c for c in df.columns if 'campaign name' in c.lower() or c.lower() == 'campaign_name'), None)
    status_col  = next((c for c in df.columns if c.lower() == 'status'), None)
    budget_col  = next((c for c in df.columns if 'budget' in c.lower()), None)
    revenue_col = next((c for c in df.columns if 'revenue' in c.lower()), None)
    orders_col  = next((c for c in df.columns if 'orders' in c.lower()), None)
    views_col   = next((c for c in df.columns if 'views' in c.lower()), None)
    clicks_col  = next((c for c in df.columns if 'clicks' in c.lower()), None)
    roi_col     = next((c for c in df.columns if c.lower() == 'roi'), None)
    cpo_col     = next((c for c in df.columns if c.lower() == 'cpo'), None)
    if not date_col or not spend_col:
        print(f"  ME Ads Summary: required columns not found in {path.name}")
        return {}, [], last_date_str
    df['_dt'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
    df = df[df['_dt'].notna()]
    df_new = df[_dt_gt(df['_dt'], last_date)]
    new_last = str(df['_dt'].max()) if len(df) else last_date_str
    print(f"  ME Ads Summary: {len(df_new)} new rows (skipping {len(df) - len(df_new)})")

    def _f(row, col, default=0.0):
        if not col:
            return default
        try:
            return abs(float(str(row.get(col, '') or '').replace(',', '') or default))
        except (ValueError, TypeError):
            return default

    def _i(row, col, default=0):
        if not col:
            return default
        try:
            return int(float(str(row.get(col, '') or '').replace(',', '') or default))
        except (ValueError, TypeError):
            return default

    monthly = {}
    campaign_rows = []
    for _, row in df_new.iterrows():
        mk = month_key(str(row['_dt']))
        if not mk:
            continue
        spend = _f(row, spend_col)
        monthly[mk] = round(monthly.get(mk, 0) + spend, 2)
        cid = str(row.get(camp_id_col, '') or '').strip() if camp_id_col else ''
        campaign_rows.append({
            'date':          str(row['_dt']),
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_nm_col, '') or '').strip() if camp_nm_col else '',
            'status':        str(row.get(status_col,  '') or '').strip() if status_col  else '',
            'budget':        _f(row, budget_col),
            'spend':         spend,
            'revenue':       _f(row, revenue_col),
            'orders':        _i(row, orders_col),
            'views':         _i(row, views_col),
            'clicks':        _i(row, clicks_col),
            'roi':           _f(row, roi_col),
            'cpo':           _f(row, cpo_col),
        })
    return monthly, campaign_rows, new_last


def process_me_ads_catalog(path, last_date_str):
    """Process ME_ADS_CATALOG daily catalog CSV.
    Returns ([catalog_rows], new_last_str)."""
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"  ME Ads Catalog: read error — {e}")
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"ME Ads Catalog read failed: {e}",
                               'impact': "this file was skipped this run — Meesho per-catalog ads data won't reflect it until a future run recovers it"})
        return [], last_date_str
    if df.empty:
        return [], last_date_str
    df.columns = [c.strip() for c in df.columns]
    date_col    = next((c for c in df.columns if c.lower() == 'date'), None)
    camp_id_col = next((c for c in df.columns if 'campaign id' in c.lower() or c.lower() == 'campaign_id'), None)
    camp_nm_col = next((c for c in df.columns if 'campaign name' in c.lower() or c.lower() == 'campaign_name'), None)
    cat_id_col  = next((c for c in df.columns if 'catalog id' in c.lower() or c.lower() == 'catalog_id'), None)
    cat_nm_col  = next((c for c in df.columns if 'catalog name' in c.lower() or c.lower() == 'catalog_name'), None)
    spend_col   = next((c for c in df.columns if 'spend' in c.lower()), None)
    revenue_col = next((c for c in df.columns if 'revenue' in c.lower() or 'sales' in c.lower()), None)
    orders_col  = next((c for c in df.columns if 'orders' in c.lower() or 'order_count' in c.lower()), None)
    views_col   = next((c for c in df.columns if 'views' in c.lower()), None)
    clicks_col  = next((c for c in df.columns if 'clicks' in c.lower()), None)
    cpc_col     = next((c for c in df.columns if 'cpc' in c.lower()), None)
    if not date_col or not cat_id_col:
        print(f"  ME Ads Catalog: required columns (date, catalog_id) not found in {path.name}")
        return [], last_date_str
    df['_dt'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
    df = df[df['_dt'].notna()]
    df_new = df[_dt_gt(df['_dt'], last_date)]
    new_last = str(df['_dt'].max()) if len(df) else last_date_str
    print(f"  ME Ads Catalog: {len(df_new)} new rows (skipping {len(df) - len(df_new)})")

    def _f(row, col, default=0.0):
        if not col:
            return default
        try:
            return float(str(row.get(col, '') or '').replace(',', '') or default)
        except (ValueError, TypeError):
            return default

    def _i(row, col, default=0):
        if not col:
            return default
        try:
            return int(float(str(row.get(col, '') or '').replace(',', '') or default))
        except (ValueError, TypeError):
            return default

    rows = []
    for _, row in df_new.iterrows():
        rows.append({
            'date':          str(row['_dt']),
            'campaign_id':   str(row.get(camp_id_col, '') or '').strip() if camp_id_col else '',
            'campaign_name': str(row.get(camp_nm_col, '') or '').strip() if camp_nm_col else '',
            'catalog_id':    str(row.get(cat_id_col,  '') or '').strip(),
            'catalog_name':  str(row.get(cat_nm_col,  '') or '').strip() if cat_nm_col else '',
            'spend':         _f(row, spend_col),
            'revenue':       _f(row, revenue_col),
            'orders':        _i(row, orders_col),
            'views':         _i(row, views_col),
            'clicks':        _i(row, clicks_col),
            'cpc':           _f(row, cpc_col),
        })
    return rows, new_last


def process_me_ads_master(path):
    """Process ME_ADS_MASTER lifetime campaign CSV.
    Returns [master_rows] — full replace each run (lifetime totals overwrite previous snapshot)."""
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"  ME Ads Master: read error — {e}")
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"ME Ads Master read failed: {e}",
                               'impact': "Meesho ads campaign master snapshot wasn't refreshed this run — may show stale campaign data until a future run recovers"})
        return []
    if df.empty:
        return []
    df.columns = [c.strip() for c in df.columns]
    camp_id_col  = next((c for c in df.columns if 'campaign id' in c.lower() or c.lower() == 'campaign_id'), None)
    camp_nm_col  = next((c for c in df.columns if 'campaign name' in c.lower() or c.lower() == 'campaign_name'), None)
    status_col   = next((c for c in df.columns if c.lower() == 'status'), None)
    budget_col   = next((c for c in df.columns if 'budget' in c.lower()), None)
    spend_col    = next((c for c in df.columns if 'spend' in c.lower() or 'budget_utilised' in c.lower()), None)
    revenue_col  = next((c for c in df.columns if 'revenue' in c.lower()), None)
    orders_col   = next((c for c in df.columns if 'orders' in c.lower() or 'order_count' in c.lower()), None)
    views_col    = next((c for c in df.columns if 'views' in c.lower() or 'impressions' in c.lower()), None)
    clicks_col   = next((c for c in df.columns if 'clicks' in c.lower()), None)
    roi_col      = next((c for c in df.columns if c.lower() == 'roi'), None)
    if not camp_id_col:
        print(f"  ME Ads Master: campaign_id column not found in {path.name}")
        return []

    def _f(row, col, default=0.0):
        if not col:
            return default
        try:
            return float(str(row.get(col, '') or '').replace(',', '') or default)
        except (ValueError, TypeError):
            return default

    def _i(row, col, default=0):
        if not col:
            return default
        try:
            return int(float(str(row.get(col, '') or '').replace(',', '') or default))
        except (ValueError, TypeError):
            return default

    rows = []
    for _, row in df.iterrows():
        cid = str(row.get(camp_id_col, '') or '').strip()
        if not cid or cid.lower() in ('nan', ''):
            continue
        rows.append({
            'campaign_id':   cid,
            'campaign_name': str(row.get(camp_nm_col, '') or '').strip() if camp_nm_col else '',
            'status':        str(row.get(status_col,  '') or '').strip() if status_col  else '',
            'budget':        _f(row, budget_col),
            'total_spend':   _f(row, spend_col),
            'total_revenue': _f(row, revenue_col),
            'total_orders':  _i(row, orders_col),
            'total_views':   _i(row, views_col),
            'total_clicks':  _i(row, clicks_col),
            'roi':           _f(row, roi_col),
        })
    print(f"  ME Ads Master: {len(rows)} campaign rows (lifetime snapshot)")
    return rows


def process_me_views(path):
    """Process ME_VIEWS daily CSV (Date, Views, Orders). Returns list of {date, views, orders}."""
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"  ME Views: read error — {e}")
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"ME Views read failed: {e}",
                               'impact': "this file was skipped this run — Meesho traffic/views won't reflect it until a future run recovers it"})
        return []
    if df.empty:
        return []
    df.columns = [c.strip() for c in df.columns]
    date_col   = next((c for c in df.columns if c.lower() == 'date'), None)
    views_col  = next((c for c in df.columns if c.lower() == 'views'), None)
    orders_col = next((c for c in df.columns if c.lower() == 'orders'), None)
    if not date_col:
        print(f"  ME Views: date column not found in {path.name}")
        return []
    df['_dt'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
    df = df[df['_dt'].notna()]
    rows = []
    for _, row in df.iterrows():
        views = orders = 0
        try:
            views  = int(float(str(row.get(views_col,  0) or 0))) if views_col  else 0
        except (ValueError, TypeError):
            pass
        try:
            orders = int(float(str(row.get(orders_col, 0) or 0))) if orders_col else 0
        except (ValueError, TypeError):
            pass
        rows.append({'date': str(row['_dt']), 'views': views, 'orders': orders})
    print(f"  ME Views: {len(rows)} date rows")
    return rows


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
    return sorted(ex.values(), key=lambda r: str(r.get(key_col, '') or ''))


# ─── Merge helpers ────────────────────────────────────────────────────────────

def merge_monthly(existing_rows, new_monthly, platform, new_sett=None, new_ads=None,
                  new_shopsy=None, new_reverse_ship=None):
    """Merge new monthly data into existing rows list.
       new_monthly:      {month: {gmv, orders, returns, settlement (optional)}}
       new_sett:         {month: settlement_float}
       new_ads:          {month: ad_spend_float}
       new_shopsy:       {month: {shopsy_orders, shopsy_revenue}}  (FK only)
       new_reverse_ship: {month: reverse_shipping_cost_float}      (FK only)
    """
    ex = {r['month']: dict(r) for r in existing_rows}

    for mk, nd in new_monthly.items():
        r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                'gmv': 0, 'settlement': 0, 'orders': 0,
                                'returns': 0, 'ad_spend': 0})
        r['gmv']      = round(float(r.get('gmv', 0) or 0) + float(nd.get('gmv', 0) or 0), 2)
        r['orders']   = int(r.get('orders', 0) or 0) + int(nd.get('orders', 0) or 0)
        r['returns']  = int(r.get('returns', 0) or 0) + int(nd.get('returns', 0) or 0)
        if 'settlement' in nd:
            r['settlement'] = round(float(r.get('settlement', 0) or 0) + float(nd['settlement'] or 0), 2)

    if new_sett:
        for mk, sett in new_sett.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['settlement'] = round(float(r.get('settlement', 0) or 0) + float(sett or 0), 2)

    if new_ads:
        for mk, ads in new_ads.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['ad_spend'] = round(float(r.get('ad_spend', 0) or 0) + float(ads or 0), 2)

    if new_shopsy:
        for mk, sh in new_shopsy.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['shopsy_orders']  = int(r.get('shopsy_orders', 0) or 0) + int(sh.get('shopsy_orders', 0) or 0)
            r['shopsy_revenue'] = round(float(r.get('shopsy_revenue', 0) or 0) + float(sh.get('shopsy_revenue', 0) or 0), 2)

    if new_reverse_ship:
        for mk, cost in new_reverse_ship.items():
            r = ex.setdefault(mk, {'month': mk, 'label': month_label(mk),
                                    'gmv': 0, 'settlement': 0, 'orders': 0,
                                    'returns': 0, 'ad_spend': 0})
            r['reverse_shipping_cost'] = round(float(r.get('reverse_shipping_cost', 0) or 0) + float(cost or 0), 2)

    return sorted(ex.values(), key=lambda r: r['month'])

def merge_me_skus(existing_rows, new_orders, new_returns, new_catalog):
    """Merge Meesho SKU data."""
    ex = {r['sku_id']: dict(r) for r in existing_rows}

    # Apply orders data
    for sid, nd in new_orders.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'total_orders': 0, 'orders': 0, 'delivered': 0, 'rto': 0, 'cust_returns': 0,
            'return_rate': 0, 'cust_ret_rate': 0, 'rto_rate': 0,
            'gmv': 0, 'avg_price': 0, 'incomplete': 0, 'wrong_product': 0, 'quality': 0
        })
        r['orders']     = _int(r.get('orders',    0)) + _int(nd.get('orders',    0))
        r['delivered']  = _int(r.get('delivered', 0)) + _int(nd.get('delivered', 0))
        r['rto']        = _int(r.get('rto',       0)) + _int(nd.get('rto',       0))
        r['gmv']        = round(_flt(r.get('gmv', 0)) + _flt(nd.get('gmv', 0)), 2)
        # total_orders/avg_price count every non-cancelled/lost order, not
        # just delivered+rto (2026-07-18: "nothing should be tied to being
        # Delivered for calculating orders and GMV" -- gmv above already
        # sums every non-cancelled order's price, so avg_price must divide
        # by that same population, not just the delivered subset, or the
        # ratio no longer means what it says). cust_returns is a downstream
        # post-delivery event on an order already counted in `orders`, kept
        # additive here unchanged from the pre-existing formula -- not
        # something this fix touches.
        total = _int(r.get('orders', 0)) + _int(r.get('cust_returns', 0))
        r['total_orders'] = total
        r['avg_price'] = round(r['gmv'] / r['orders'], 2) if r['orders'] else 0

    # Apply returns data
    for sid, nd in new_returns.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': sid, 'type': '',
            'total_orders': 0, 'delivered': 0, 'rto': 0, 'cust_returns': 0,
            'return_rate': 0, 'cust_ret_rate': 0, 'rto_rate': 0,
            'gmv': 0, 'avg_price': 0, 'incomplete': 0, 'wrong_product': 0, 'quality': 0
        })
        r['cust_returns']  = _int(r.get('cust_returns',  0)) + _int(nd.get('cust_returns',  0))
        r['incomplete']    = _int(r.get('incomplete',    0)) + _int(nd.get('incomplete',    0))
        r['wrong_product'] = _int(r.get('wrong_product', 0)) + _int(nd.get('wrong_product', 0))
        r['quality']       = _int(r.get('quality',       0)) + _int(nd.get('quality',       0))

    # Apply catalog stock — new_catalog may be {sku_id: stock_int} or {sku_id: {listings:[...]}}
    for sid, stock in new_catalog.items():
        if sid in ex:
            if isinstance(stock, dict):
                stock = sum(l.get('stock', 0) for l in stock.get('listings', []))
            ex[sid]['stock'] = stock

    # Recalculate rates
    for sid, r in ex.items():
        # `orders` is a new field (2026-07-18) -- doesn't exist yet on SKUs
        # persisted before this fix, and won't get backfilled for a
        # dormant/discontinued SKU that never appears in a fresh
        # new_orders entry again. Falling back to the old delivered+rto
        # formula when `orders` is genuinely absent (not just zero from a
        # real zero-order SKU -- 'orders' in r distinguishes "never set"
        # from "set to 0") avoids silently zeroing out total_orders for
        # every existing SKU the first time this runs, until each one
        # happens to get a fresh order naturally.
        _orders_ct = _int(r['orders']) if 'orders' in r else (_int(r.get('delivered', 0)) + _int(r.get('rto', 0)))
        total = _orders_ct + _int(r.get('cust_returns', 0))
        r['total_orders'] = total
        if total:
            r['rto_rate']      = round(_int(r.get('rto',          0)) / total * 100, 2)
            r['cust_ret_rate'] = round(_int(r.get('cust_returns',  0)) / total * 100, 2)
            r['return_rate']   = round((_int(r.get('rto', 0)) + _int(r.get('cust_returns', 0))) / total * 100, 2)
        else:
            r['rto_rate'] = r['cust_ret_rate'] = r['return_rate'] = 0

    return sorted(ex.values(), key=lambda r: -r.get('gmv', 0))

def merge_fk_skus(existing_rows, new_payments, new_views, new_reverse_ship=None):
    """Merge FK SKU data."""
    ex = {r['sku_id']: dict(r) for r in existing_rows}

    for sid, nd in new_payments.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'mrp': 0, 'selling': 0, 'settlement': 0, 'stock': 0,
            'ctr': 0, 'ad_revenue': 0, 'ad_spend': 0, 'roas': 0,
            'conversions': 0, 'ad_views': 0,
            'reverse_shipping_fee': 0,
        })
        r['orders']     = _int(r.get('orders',     0)) + _int(nd.get('orders',     0))
        r['returns']    = _int(r.get('returns',    0)) + _int(nd.get('returns',    0))
        r['gmv']        = round(_flt(r.get('gmv', 0)) + _flt(nd.get('gmv', 0)), 2)
        r['settlement'] = round(_flt(r.get('settlement', 0)) + _flt(nd.get('settlement', 0)), 2)
        r['conversions']= _int(r.get('conversions', 0)) + _int(nd.get('orders',   0))

    for sid, nd in new_views.items():
        r = ex.setdefault(sid, {
            'sku_id': sid, 'name': nd['name'], 'type': '',
            'mrp': 0, 'selling': 0, 'settlement': 0, 'stock': 0,
            'ctr': 0, 'ad_revenue': 0, 'ad_spend': 0, 'roas': 0,
            'conversions': 0, 'ad_views': 0,
            'reverse_shipping_fee': 0,
        })
        r['ad_views']   = _int(r.get('ad_views', 0)) + _int(nd.get('ad_views', 0))
        r['ad_revenue'] = round(_flt(r.get('ad_revenue', 0)) + _flt(nd.get('ad_revenue', 0)), 2)
        r['ad_spend']   = round(_flt(r.get('ad_spend',   0)) + _flt(nd.get('ad_spend',   0)), 2)
        r['roas']       = round(r['ad_revenue'] / r['ad_spend'], 4) if r['ad_spend'] else 0.0
        total_views = r['ad_views']
        clicks = _int(r.get('clicks', 0)) + _int(nd.get('clicks', 0))
        r['clicks'] = clicks
        r['ctr'] = round(clicks / total_views * 100, 2) if total_views else 0

    if new_reverse_ship:
        for sid, cost in new_reverse_ship.items():
            if sid in ex:
                # _flt guards against a stale non-numeric value already stored
                # in the CSV for this field (pre-existing data-quality issue,
                # unrelated to the 2026-07-03 product_master rebuild).
                ex[sid]['reverse_shipping_fee'] = round(
                    _flt(ex[sid].get('reverse_shipping_fee', 0)) + _flt(cost), 2)

    return sorted(ex.values(), key=lambda r: -r.get('gmv', 0))

def build_return_reasons(existing_rows, new_reasons):
    """Merge return reason counts and compute percentages."""
    ex = {r['reason']: _int(r.get('count', 0)) for r in existing_rows}
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
        r['views']   = _int(r.get('views',   0)) + _int(nd.get('views',   0))
        r['clicks']  = _int(r.get('clicks',  0)) + _int(nd.get('clicks',  0))
        r['orders']  = _int(r.get('orders',  0)) + _int(nd.get('orders',  0))
        r['revenue'] = round(_flt(r.get('revenue', 0)) + _flt(nd.get('revenue', 0)), 2)
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
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"build_me_daily orders read failed: {e}",
                               'impact': "this file's rows are missing from the Meesho daily orders trend chart"})
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
        # Delivered Date (closure) preferred over Return Created Date
        # (initiated) -- same fix as process_meesho_returns, active.md #70,
        # 2026-07-20, kept consistent here since this feeds me_daily's own
        # returns_received/top_return_reason chart. Per-ROW fallback, not
        # per-file (independent code review finding, 2026-07-20) -- see the
        # identical comment in process_meesho_returns for why.
        delivered_col = next((c for c in df.columns if 'Delivered Date' in c), None)
        created_col   = next((c for c in df.columns if 'Return Created Date' in c), None)
        sku_col  = next((c for c in df.columns if c == 'SKU'), 'SKU')
        reason_col     = next((c for c in df.columns if 'Detailed Return Reason' in c), None)
        sub_reason_col = next((c for c in df.columns if 'Return Reason' in c
                               and 'Detailed' not in c), None)
        if delivered_col or created_col:
            _dt_delivered = pd.to_datetime(df[delivered_col], errors='coerce').dt.date if delivered_col else pd.Series(pd.NaT, index=df.index)
            _dt_created   = pd.to_datetime(df[created_col],   errors='coerce').dt.date if created_col   else pd.Series(pd.NaT, index=df.index)
            df['_dt'] = _dt_delivered.combine_first(_dt_created)
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
        _run_warnings.append({'file': str(path), 'type': 'ME', 'reason': f"build_me_daily returns read failed: {e}",
                               'impact': "this file's rows are missing from the Meesho daily returns trend chart"})
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
    qty_col    = 'Quantity'
    source_col = 'Order source'

    orders_df['_sid']    = orders_df[sku_col].astype(str).apply(
        lambda x: me_sku_id(x.strip())[0])
    orders_df['_sname']  = orders_df[sku_col].astype(str).apply(
        lambda x: me_sku_id(x.strip())[1])
    orders_df['_status'] = orders_df[status_col].astype(str).str.strip()
    orders_df['_price']  = pd.to_numeric(
        orders_df[price_col], errors='coerce').fillna(0)
    orders_df['_qty']    = pd.to_numeric(
        orders_df.get(qty_col, 1), errors='coerce').fillna(1).astype(int) \
        if qty_col in orders_df.columns else 1
    orders_df['_is_ad']  = (
        orders_df[source_col].astype(str).str.strip() == 'Ad order'
    ) if source_col in orders_df.columns else False
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
        # gmv counts every non-cancelled/lost row, not just DELIVERED
        # (2026-07-18: "nothing should be tied to being Delivered for
        # calculating the number of orders and GMV" -- same exclusion
        # shippable_units already uses below).
        gmv       = round(float(
            grp.loc[~statuses.isin(['CANCELLED', 'LOST']), '_price'].sum()), 2)
        total_units = int(grp['_qty'].sum()) if '_qty' in grp.columns else len(grp)
        # shippable_units (added 2026-07-17, active.md item #64 -- sale-
        # triggered stock decrement) -- total_units above includes EVERY
        # row regardless of status, including CANCELLED/LOST, which were
        # never actually shipped and must not decrement stock. Same
        # CANCELLED/LOST exclusion already used for `cancelled` above, just
        # applied to quantity instead of order-count. Deliberately does NOT
        # exclude RTO_COMPLETE -- an RTO order genuinely was shipped first;
        # stock credit-back for a returned item is a separate mechanism
        # (Return Receipts intact/damaged), not a reason to skip the
        # decrement at ship time.
        shippable_units = int(grp.loc[~statuses.isin(['CANCELLED', 'LOST']), '_qty'].sum()) \
            if '_qty' in grp.columns else (len(grp) - cancelled)
        ad_orders   = int(grp['_is_ad'].sum()) if '_is_ad' in grp.columns else 0
        daily[(str(dt), sid)] = {
            'date': str(dt), 'sku_id': sid, 'sku_name': sname,
            'orders_placed': len(grp),
            'delivered': delivered, 'rto': rto, 'cancelled': cancelled,
            'gmv': gmv,
            'returns_received': 0, 'top_return_reason': '', 'states': '',
            'total_units': total_units, 'shippable_units': shippable_units, 'ad_orders': ad_orders,
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
                        'states': '', 'total_units': 0, 'shippable_units': 0, 'ad_orders': 0,
                    }
            cur += timedelta(days=1)

    rows = sorted(daily.values(), key=lambda r: (r['date'], r['sku_id']))
    if rows:
        n_skus = len({r['sku_id'] for r in rows})
        print(f"    build_me_daily: {len(rows)} rows "
              f"({rows[0]['date']} to {rows[-1]['date']}, "
              f"{n_skus} SKUs)")
    return rows


def build_me_state_summary(orders_paths):
    """
    Build state-level order summary from raw ME orders files.
    Returns list of dicts matching me_state_summary schema.
    NOTE: returns only data from the current batch of files — caller must merge
    with existing rows using merge_me_state_summary().
    """
    if not orders_paths:
        return []

    raw_dfs = [_read_me_orders_raw(p) for p in orders_paths]
    df = pd.concat([d for d in raw_dfs if not d.empty], ignore_index=True)
    if df.empty:
        return []

    status_col = 'Reason for Credit Entry'
    price_col  = 'Supplier Discounted Price (Incl GST and Commision)'
    state_col  = 'Customer State'
    sku_col    = 'SKU'

    if state_col not in df.columns:
        return []

    df['_status'] = df[status_col].astype(str).str.strip()
    df['_price']  = pd.to_numeric(df[price_col], errors='coerce').fillna(0)
    df['_state']  = df[state_col].astype(str).str.strip()
    df['_sid']    = df[sku_col].astype(str).apply(lambda x: me_sku_id(x.strip())[0])

    df = df[(df['_state'].str.len() > 0) & (df['_state'] != 'nan')]
    if df.empty:
        return []

    states = {}
    for state, grp in df.groupby('_state'):
        statuses  = grp['_status']
        delivered = int((statuses == 'DELIVERED').sum())
        rto       = int((statuses == 'RTO_COMPLETE').sum())
        # orders/gmv exclude only CANCELLED/LOST, not gated on DELIVERED
        # (2026-07-18, same fix as build_me_daily/process_meesho_orders --
        # `orders` here previously counted every row including cancelled
        # ones, which is its own separate inconsistency fixed the same way
        # for a coherent "orders" definition across the codebase).
        _not_dead = ~statuses.isin(['CANCELLED', 'LOST'])
        orders    = int(_not_dead.sum())
        gmv       = round(float(grp.loc[_not_dead, '_price'].sum()), 2)
        top_skus  = grp['_sid'].value_counts().head(5).index.tolist()
        total_fin = delivered + rto
        states[state] = {
            'state':        state,
            'orders':       orders,
            'delivered':    delivered,
            'rto':          rto,
            'rto_rate_pct': round(rto / total_fin * 100, 2) if total_fin else 0,
            'gmv':          gmv,
            'top_skus':     '|'.join(top_skus),
        }

    print(f"    build_me_state_summary: {len(states)} states from {len(orders_paths)} file(s)")
    return list(states.values())


def merge_me_state_summary(existing_rows, new_rows):
    """Merge new state rows into existing cumulative state summary."""
    ex = {r['state']: dict(r) for r in existing_rows}
    for nd in new_rows:
        state = nd['state']
        r = ex.setdefault(state, {
            'state': state, 'orders': 0, 'delivered': 0,
            'rto': 0, 'rto_rate_pct': 0, 'gmv': 0, 'top_skus': '',
        })
        r['orders']    += nd['orders']
        r['delivered'] += nd['delivered']
        r['rto']       += nd['rto']
        r['gmv']        = round(r['gmv'] + nd['gmv'], 2)
        total_fin = r['delivered'] + r['rto']
        r['rto_rate_pct'] = round(r['rto'] / total_fin * 100, 2) if total_fin else 0
        if nd.get('top_skus'):
            r['top_skus'] = nd['top_skus']
    return sorted(ex.values(), key=lambda r: -r['orders'])


def merge_fk_zone_summary(existing_rows, new_zone_counts):
    """Merge FK shipping zone data into cumulative zone summary."""
    ex = {r['zone']: dict(r) for r in existing_rows}
    for zone, nd in new_zone_counts.items():
        r = ex.setdefault(zone, {'zone': zone, 'orders': 0, 'revenue': 0.0, 'returns': 0})
        r['orders']  += nd['orders']
        r['revenue']  = round(r['revenue'] + nd['revenue'], 2)
        r['returns'] += nd['returns']
    for r in ex.values():
        r['return_rate_pct'] = round(r['returns'] / r['orders'] * 100, 2) if r['orders'] else 0
    return sorted(ex.values(), key=lambda r: -r['orders'])


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
            _run_warnings.append({'file': str(p), 'type': 'FK', 'reason': f"build_fk_daily read failed: {e}",
                                   'impact': "this file's rows are missing from the Flipkart daily views/orders trend chart"})

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
            _run_warnings.append({'file': str(p), 'type': 'FK', 'reason': f"build_fk_keywords read failed: {e}",
                                   'impact': "this file's keyword rows are missing from the Flipkart Keywords tab"})

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
    Does NOT touch rumee_db_summary.csv or rumee_db_daily.csv.
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

    # Collect raw files — temporarily ignore processed_file / processed_modified cache
    orig_config = list(db.get('config', []))
    db['config'] = [r for r in orig_config
                    if not str(r.get('key', '')).startswith('processed_file:')
                    and not str(r.get('key', '')).startswith('processed_modified:')]

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

    for fp, ft_hint, _ in source_files:
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
                _run_warnings.append({'file': 'alltime_ready_email', 'type': 'INFRA', 'reason': f"all-time-ready notification email failed to send: {e}",
                                       'impact': "cosmetic only — the email notification didn't go out, but the all-time data itself was generated fine"})


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
        '--az-backfill-start', default=None, metavar='YYYY-MM-DD',
        help='One-off Amazon Orders/Returns backfill: sets az_orders_last_date and '
             'az_returns_last_date to the day before this date, so the next '
             '_az_request_report call requests starting exactly from this date '
             '(createReport\'s own 30-day span cap still applies). Used for the '
             '2026-07-14 real June-2026 verification (dashboard memory active.md #57). '
             'Does not touch any other stream.'
    )
    parser.add_argument(
        '--set-ledger-sheet-id', default=None, metavar='SHEET_ID',
        help='One-off: sets the ledger_sheet_id config key to a Google Sheet Jaiswal '
             'has manually created and shared with the service account as Editor '
             '(the service account cannot create new Sheets itself -- 2026-07-14, '
             'dashboard memory active.md #55/#57, zero Drive storage quota on a bare '
             'service account). Sheet must already have an \'orders\' tab with the '
             'LEDGER_COLUMNS header row -- see sheets_connector.py.'
    )
    parser.add_argument(
        '--generate-alltime', action='store_true',
        help='(future) Generate alltime data snapshot after processing'
    )
    parser.add_argument(
        '--seed-users', action='store_true',
        help='One-time bootstrap for the whole-dashboard login + roles feature '
             '(dashboard memory active.md #41): writes rumee_users/rumeein@gmail.com '
             "(role='owner') via the Admin SDK. Must be run exactly once BEFORE the "
             'role-based firestore.rules are published -- those rules check this '
             'collection to decide who is an owner, so nobody (including the real '
             'owner) could pass that check if the doc did not already exist the '
             'moment the rules go live. Safe to re-run (idempotent upsert). Does not '
             'touch any pipeline data table -- exits immediately after writing.'
    )
    return parser.parse_args()


def _scan_local_files():
    """Return [(path, None)] for every CSV/XLSX in new_data/."""
    NEW_DATA.mkdir(exist_ok=True)
    files = [
        f for f in NEW_DATA.iterdir()
        if f.is_file() and f.suffix.lower() in ('.csv', '.xlsx', '.xls')
    ]
    return [(f, None, '') for f in sorted(files)]


# ─── Order-status daily rollup (active.md item #67, 2026-07-18) ──────────────

def _status_daily_rollup(order_sku_index_rows):
    """
    Groups a persisted order_id-keyed sku-index table (fk_order_sku_index /
    me_order_sku_index -- full unbounded history, re-derived every run) into
    per-(order_date, status) order counts. Powers the dashboard's per-status
    trend filter -- kept as a small separate table rather than pushing the
    raw per-order index to Firestore, since that grows unbounded.
    """
    counts = {}
    for r in order_sku_index_rows:
        d = r.get('order_date', '')
        if not d:
            continue
        st = r.get('status', 'placed')
        counts[(d, st)] = counts.get((d, st), 0) + 1
    return [{'date': d, 'status': st, 'orders': n} for (d, st), n in sorted(counts.items())]


# ─── Amazon monthly rollup ────────────────────────────────────────────────────

def _az_monthly_rollup(az_orders_daily_rows, az_returns_daily_rows=None, az_settlement_rows=None):
    """
    Derives az_monthly (month, label, gmv, orders, returns, settlement) from
    az_orders_daily/az_returns_daily/az_settlement. Replaces the old
    process_az_monthly, which made its own live SP-API Orders v0 call every
    run just to get a monthly GMV/order-count total -- redundant once
    az_orders_daily carried full per-order history (removed 2026-07-15,
    Jaiswal, once Amazon Orders/Settlement/Returns were confirmed working
    against real June 2026 data). Always a full recompute, not an upsert --
    the 3 source tables already hold unwindowed full history.

    returns/settlement added 2026-07-17 (Master tab parity, dashboard memory
    active.md item #62) -- settlement is joined by order_date the same way
    the Firestore rumee_az_settlement push does (az_settlement itself has no
    date column). ad_spend deliberately NOT added here -- no data source
    exists yet (pending Amazon Advertising API), stays absent/0 downstream,
    same as the Amazon tab's own "pending" note already says.
    """
    az_returns_daily_rows = az_returns_daily_rows or []
    az_settlement_rows    = az_settlement_rows or []

    monthly = {}
    for r in az_orders_daily_rows:
        dt = r.get('order_date', '')
        if not dt:
            continue
        mk = dt[:7]
        m = monthly.setdefault(mk, {'gmv': 0.0, 'orders': 0, 'returns': 0, 'settlement': 0.0})
        m['gmv']    += float(r.get('gmv', 0) or 0)
        m['orders'] += 1

    for r in az_returns_daily_rows:
        dt = r.get('return_date', '')
        if not dt:
            continue
        mk = dt[:7]
        m = monthly.setdefault(mk, {'gmv': 0.0, 'orders': 0, 'returns': 0, 'settlement': 0.0})
        m['returns'] += 1

    _az_order_dates = {r['order_id']: r.get('order_date', '') for r in az_orders_daily_rows}
    for r in az_settlement_rows:
        dt = _az_order_dates.get(r.get('order_id', ''), '')
        if not dt:
            continue
        mk = dt[:7]
        m = monthly.setdefault(mk, {'gmv': 0.0, 'orders': 0, 'returns': 0, 'settlement': 0.0})
        m['settlement'] += float(r.get('settlement', 0) or 0)

    rows = []
    for mk, v in monthly.items():
        label = datetime.strptime(mk, '%Y-%m').strftime('%b %Y')
        rows.append({
            'month':      mk,
            'label':      label,
            'gmv':        round(v['gmv'], 2),
            'orders':     v['orders'],
            'returns':    v['returns'],
            'settlement': round(v['settlement'], 2),
            'ad_spend': 0,
        })
    return sorted(rows, key=lambda r: r['month'])


def send_discord_az_notification(summary):
    """
    Post an Amazon SP-API run summary embed to the #pipeline Discord channel.
    Fires every pipeline run regardless of outcome. Restores the visibility
    the old process_az_monthly-era notification gave (Jaiswal said it was
    helpful and asked for it back, 2026-07-15, after that function -- and
    its bundled notifier -- was retired as dead code). Reports the current
    request/poll/settlement/ledger flow instead of the old live Orders v0 call.

    summary: {
        'orders_req', 'orders_poll': str, 'orders_rows': int,
        'returns_req', 'returns_poll': str, 'returns_rows': int,
        'sqp_req', 'sqp_poll': str, 'sqp_rows': int,
        'catalog_req', 'catalog_poll': str, 'catalog_rows': int,
        'settlement_status': str, 'settlement_rows': int,
        'ledger_ran': bool, 'ledger_inserted': int, 'ledger_updated': int,
        'errors': [str], 'warnings': [str],
    }
    """
    import urllib.request, urllib.error

    WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL_PIPELINE')
    if not WEBHOOK_URL:
        try:
            from rumee_secrets import DISCORD_WEBHOOK_URL
            WEBHOOK_URL = DISCORD_WEBHOOK_URL
        except ImportError:
            return

    errors   = summary.get('errors', [])
    warnings = summary.get('warnings', [])
    colour = 0xe74c3c if errors else (0xe67e22 if warnings else 0x27ae60)
    status_label = ('❌ Errors this run' if errors else
                     '⚠️ Warnings this run' if warnings else '✅ Clean run')

    fields = [
        {'name': 'Status', 'value': status_label, 'inline': False},
        {'name': 'Orders',
         'value': f"request: {summary.get('orders_req','?')} | check: {summary.get('orders_poll','?')} | {summary.get('orders_rows',0)} new row(s)",
         'inline': False},
        {'name': 'Returns',
         'value': f"request: {summary.get('returns_req','?')} | check: {summary.get('returns_poll','?')} | {summary.get('returns_rows',0)} new row(s)",
         'inline': False},
        {'name': 'Search Query Performance',
         'value': f"request: {summary.get('sqp_req','?')} | check: {summary.get('sqp_poll','?')} | {summary.get('sqp_rows',0)} new row(s)",
         'inline': False},
        {'name': 'Catalog',
         'value': f"request: {summary.get('catalog_req','?')} | check: {summary.get('catalog_poll','?')} | {summary.get('catalog_rows',0)} listing(s)",
         'inline': False},
        {'name': 'Settlement',
         'value': f"{summary.get('settlement_status','?')} | {summary.get('settlement_rows',0)} order(s) with fee data",
         'inline': False},
        {'name': 'Orders Ledger',
         'value': (f"{summary.get('ledger_inserted',0)} inserted, {summary.get('ledger_updated',0)} updated"
                   if summary.get('ledger_ran') else 'not run — no orders in window this run'),
         'inline': False},
    ]
    if warnings:
        fields.append({'name': f'⚠️ Warnings ({len(warnings)})',
                        'value': '\n'.join(f'• {w}' for w in warnings)[:1000], 'inline': False})
    if errors:
        fields.append({'name': f'❌ Errors ({len(errors)})',
                        'value': '\n'.join(f'• {e}' for e in errors)[:1000], 'inline': False})

    embed = {
        'title':  f'📦 Amazon SP-API — {date.today().isoformat()}',
        'color':  colour,
        'fields': fields,
    }
    payload = json.dumps({'embeds': [embed]}).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'RumeePipeline/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"  [Amazon] Discord notification sent (HTTP {resp.status})")
    except urllib.error.URLError as e:
        print(f"  [Amazon] Discord notification failed: {e}")


# ─── Amazon Orders/Settlement/Returns (Reports API, dashboard memory ─────────
# active.md item #57, 2026-07-14) — replaces the old monthly-aggregate-only
# design above with per-order data, matching the FK/ME Orders Ledger pattern.
# Column names are matched FLEXIBLY (keyword-based, like process_fk_payments'
# _fkp_str/_fkp_num) because none of the three report shapes have been
# verified against a real downloaded file yet — Amazon's own docs describe
# columns in prose, not verbatim headers. Real-file verification is still a
# TODO once this runs live; these parsers are built defensively (skip/blank
# rather than crash) specifically because of that uncertainty.

def _az_find_col(columns, *needles):
    """Shared flexible column finder for all 3 Amazon parsers below — first
    (already-lowercased) column name containing every needle."""
    return next((c for c in columns if all(n in c for n in needles)), None)


def process_az_orders_report(content, last_date_str):
    """
    Parses GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL (tab-separated).

    Returns:
        monthly:       {month: {gmv, orders}}
        skus:          {sku: {name, orders, gmv}}
        order_rows:    per-order dicts for the Orders Ledger (platform='AZ')
        new_last_date: str
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(io.StringIO(content), sep='\t', dtype=str, on_bad_lines='skip')
    if df.empty:
        return {}, {}, [], last_date_str
    df.columns = [c.strip().lower() for c in df.columns]

    oid_col   = _az_find_col(df.columns, 'order', 'id')
    date_col  = _az_find_col(df.columns, 'purchase', 'date') or _az_find_col(df.columns, 'order', 'date')
    sku_col   = _az_find_col(df.columns, 'sku')
    qty_col   = _az_find_col(df.columns, 'quantity')
    price_col = _az_find_col(df.columns, 'item', 'price')

    if not oid_col or not date_col:
        # Raise rather than silently return empty — the caller advances the
        # watermark to the requested range's end once a report downloads
        # successfully (the report is authoritative for its whole window),
        # so a silent empty-return here would look identical to "genuinely
        # zero orders this period" and permanently skip real data instead of
        # retrying (dashboard memory active.md #57 review finding).
        raise ValueError(f"required columns not found (order_id={oid_col}, date={date_col})")

    dates = pd.to_datetime(df[date_col], errors='coerce').dt.date
    valid = dates.notna()
    df2   = df[valid].copy()
    df2['_dt'] = dates[valid].values
    df_new = df2[df2['_dt'] > last_date]

    if df_new.empty:
        print(f"  AZ Orders: 0 new rows (last={last_date_str})")
        return {}, {}, [], last_date_str

    monthly    = {}
    skus       = {}
    order_rows = []
    for _, row in df_new.iterrows():
        oid = str(row.get(oid_col, '') or '').strip()
        if not oid or oid.lower() == 'nan':
            continue
        dt  = row['_dt']
        sku = str(row.get(sku_col, '') or '').strip() if sku_col else ''
        try:
            qty = int(float(row.get(qty_col, 1) or 1)) if qty_col else 1
        except (ValueError, TypeError):
            qty = 1
        try:
            # A blank item-price cell reads as pandas NaN even under
            # dtype=str; NaN is truthy (so `or 0` never substitutes) and
            # float(nan) succeeds rather than raising, so this needs an
            # explicit check rather than relying on the except guard below
            # (dashboard memory active.md #57 review finding).
            _raw_price = row.get(price_col, 0) if price_col else 0
            price = 0.0 if pd.isna(_raw_price) else float(_raw_price or 0)
        except (ValueError, TypeError):
            price = 0.0

        mk = dt.strftime('%Y-%m')
        m = monthly.setdefault(mk, {'gmv': 0, 'orders': 0})
        m['gmv'] += price
        m['orders'] += 1
        s = skus.setdefault(sku, {'name': sku, 'orders': 0, 'gmv': 0})
        s['orders'] += 1
        s['gmv'] += price

        order_rows.append({
            'order_id':   oid,
            'order_date': dt.isoformat(),
            'platform':   'AZ',
            'sku':        sku,
            'qty':        qty,
            'gmv':        round(price, 2),
            'settlement': 0.0,   # filled in from the settlement report join in build_az_ledger_rows
            'commission': 0.0, 'fixed_fee': 0.0, 'collection_fee': 0.0,
            'shipping_fwd': 0.0, 'shipping_rev': 0.0, 'gst_on_fees': 0.0,
            'tcs': 0.0, 'tds': 0.0, 'penalty': 0.0,
            # 'placed', not 'Delivered' -- Amazon's Orders API has no
            # DELIVERED concept at all (confirmed against official SP-API
            # docs, active.md item #66, 2026-07-18). This is the provisional
            # fallback; _az_apply_settlement_status overrides it with
            # 'return' once a Refund transaction-type line appears against
            # this order in a Settlement report (the authoritative source).
            'status':     'placed',
            'zone':       '',
            'is_shopsy':  '',
        })

    new_last = df_new['_dt'].max()
    print(f"  AZ Orders: {len(order_rows)} new rows ({last_date_str} -> {new_last.isoformat()})")
    return monthly, skus, order_rows, new_last.isoformat()


def process_az_settlement_report(content):
    """
    Parses GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2 — a long-format
    amount-type/amount-description/amount table per order (confirmed shape
    from live docs, 2026-07-14), NOT the deprecated V1 flat file.

    'settlement' (the sum of every amount line for an order) is the one
    reliable figure regardless of label-matching accuracy — it's the actual
    net amount Amazon settled for that order.

    The commission/shipping_fwd/tcs/fixed_fee breakdown below was verified
    2026-07-14 against a REAL downloaded settlement report (10 real reports,
    June-July 2026) — confirmed real amount-type/amount-description pairs:
      ('ItemPrice', 'Principal')              -- the sale amount, not a fee
      ('ItemPrice', 'Product Tax')             -- GST on the item (catch-all)
      ('ItemTCS', 'TCS-IGST')                  -- Tax Collected at Source
      ('ItemFees', 'Fixed closing fee')        -- genuine fixed fee
      ('ItemFees', 'Fixed closing fee IGST')   -- GST on the fixed fee
      ('ItemFees', 'Refund commission')        -- commission reversal on a return
      ('ItemFees', 'Refund commission IGST')   -- GST on the above
      ('other-transaction', 'Amazon Easy Ship Charges')       -- shipping (self-ship via Amazon's Easy Ship)
      ('other-transaction', 'MFNPostagePurchaseCompleteIGST') -- GST on the above
    The first real test run showed shipping_fwd staying at 0 despite a real
    Easy Ship Charges line being present — root cause: this code was matching
    on the substring 'shipping', but Amazon's real label says 'Ship', not
    'Shipping'. Fixed below. A real 'Commission' line (non-refund) has not
    yet been seen in this seller's data — only 'Refund commission' has,
    which already matches correctly.

    Returns: (fees, refunded_order_ids)
      fees:               {order_id: {settlement, commission, shipping_fwd, tcs, fixed_fee}}
      refunded_order_ids: set of order_ids with at least one transaction-type=='Refund'
                           line (active.md item #66, 2026-07-18) -- confirmed against a
                           real live settlement report: 'transaction-type' is a clean
                           per-line column ('Order'/'Refund'/'other-transaction') that
                           the fee-label matching above never read. An order's presence
                           here is the authoritative "this was returned" signal, always
                           overriding the Orders report's status (which has no concept
                           of delivery/return at all -- confirmed against official
                           SP-API docs, Amazon's OrderStatus enum has no DELIVERED value).
    """
    df = pd.read_csv(io.StringIO(content), sep='\t', dtype=str, on_bad_lines='skip')
    if df.empty:
        return {}, set()
    df.columns = [c.strip().lower() for c in df.columns]

    oid_col   = next((c for c in df.columns if 'order' in c and 'id' in c and 'merchant' not in c), None)
    type_col  = next((c for c in df.columns if 'amount' in c and 'type' in c), None)
    desc_col  = next((c for c in df.columns if 'amount' in c and 'description' in c), None)
    txn_col   = next((c for c in df.columns if c == 'transaction-type'), None) or \
                next((c for c in df.columns if 'transaction' in c and 'type' in c), None)
    amt_col   = next((c for c in df.columns if c == 'amount'), None) or \
               next((c for c in df.columns if 'amount' in c and 'type' not in c and 'description' not in c), None)

    if not oid_col or not amt_col:
        # Raise rather than silently return {} — see the identical note in
        # process_az_orders_report. For settlement specifically this matters
        # even more: _az_acquire_settlement advances az_settlement_last_created
        # from the report's OWN createdTime metadata, independent of whether
        # parsing here succeeds, so main() must be able to tell "this
        # specific report failed to parse" apart from "it parsed to zero
        # fee lines" in order to only advance the watermark past reports it
        # actually processed (see the caller in main()).
        raise ValueError(f"required columns not found (order_id={oid_col}, amount={amt_col})")

    fees = {}
    refunded_order_ids = set()
    for _, row in df.iterrows():
        oid = str(row.get(oid_col, '') or '').strip()
        if not oid or oid.lower() == 'nan':
            continue
        if txn_col and str(row.get(txn_col, '') or '').strip() == 'Refund':
            refunded_order_ids.add(oid)
        try:
            amt = float(row.get(amt_col, 0) or 0)
        except (ValueError, TypeError):
            continue

        label = ((str(row.get(desc_col, '') or '') if desc_col else '') + ' ' +
                 (str(row.get(type_col, '') or '') if type_col else '')).strip().lower()

        f = fees.setdefault(oid, {'settlement': 0.0, 'commission': 0.0, 'shipping_fwd': 0.0, 'tcs': 0.0, 'fixed_fee': 0.0})
        f['settlement'] += amt   # trustworthy regardless of label-matching below
        cost = -amt if amt < 0 else 0.0   # fee/charge lines are typically posted negative
        if 'commission' in label or 'referral' in label:
            f['commission'] += cost
        elif 'ship' in label and 'tax' not in label:
            f['shipping_fwd'] += cost
        elif 'tcs' in label:
            f['tcs'] += abs(amt)   # stored as a positive magnitude, matching the FK tcs convention (process_fk_payments)
        elif 'principal' in label or 'itemprice' in label.replace(' ', ''):
            pass   # this is the sale amount itself, already captured by the Orders report — not a fee
        elif cost:
            f['fixed_fee'] += cost   # unrecognized fee line — catch-all so nothing silently drops

    print(f"  AZ Settlement: {len(fees)} orders with settlement data, {len(refunded_order_ids)} refunded")
    return fees, refunded_order_ids


def process_az_returns_report(content, last_date_str):
    """
    Parses GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE — confirmed (live docs,
    2026-07-14) to cover merchant-fulfilled/self-ship orders, not FBA-only.

    Bucketed by RETURN DELIVERY DATE (when the returned item was physically
    received back — closure), not Return Request Date (when the customer
    started the return) — active.md item #70, 2026-07-20. Falls back to
    Return Request Date for a row with no delivery date recorded yet.

    Returns:
        reasons:                {reason_str: count}
        new_last_date:           str
        az_return_reason_index:  {order_id: reason_str}
        az_order_awb_index:      {order_id: tracking_id} — the report's own
            carrier tracking ID column, used to resolve a Return Receipts
            scan that only captured the AWB (not the Order ID) back to this
            order, same pattern as the FK/ME fix (dashboard memory #55).
        return_rows:             per-row dicts (order_id, return_date,
            return_reason, tracking_id, sku) for persistence into
            db['az_returns_daily'] and Data Pipeline Map gap-tracking. `sku`
            (from the report's own "Merchant SKU" column -- confirmed via
            Amazon's official SP-API report-schema docs, 2026-07-21, not
            guessed) added for dashboard memory active.md item #72, so the
            Returns Scanner's SKU lookup can prefer the RETURN's own reported
            SKU over the order-placement-time guess once a return has synced.
    """
    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
    df = pd.read_csv(io.StringIO(content), sep='\t', dtype=str, on_bad_lines='skip')
    if df.empty:
        return {}, last_date_str, {}, {}, []
    df.columns = [c.strip().lower() for c in df.columns]

    oid_col      = _az_find_col(df.columns, 'order', 'id')
    delivery_col = _az_find_col(df.columns, 'return', 'delivery', 'date')
    request_col  = (_az_find_col(df.columns, 'return', 'request', 'date')
                     or _az_find_col(df.columns, 'return', 'date'))
    reason_col = _az_find_col(df.columns, 'return', 'reason')
    track_col  = _az_find_col(df.columns, 'tracking')
    sku_col    = _az_find_col(df.columns, 'merchant', 'sku')

    if not oid_col or (not delivery_col and not request_col):
        # Raise rather than silently return empty — see the identical note
        # in process_az_orders_report (dashboard memory active.md #57 review
        # finding: a silent empty-return here would let the watermark
        # advance past unparsed real data instead of retrying).
        raise ValueError(f"required columns not found (order_id={oid_col}, date={delivery_col or request_col})")

    # Per-ROW fallback (delivery date preferred, request date for a row not
    # yet delivered back), not per-file (independent code review finding,
    # 2026-07-20) -- picking one column for the whole file would silently
    # drop every not-yet-delivered row via the notna() filter below.
    _dt_delivery = pd.to_datetime(df[delivery_col], errors='coerce').dt.date if delivery_col else pd.Series(pd.NaT, index=df.index)
    _dt_request  = pd.to_datetime(df[request_col],  errors='coerce').dt.date if request_col  else pd.Series(pd.NaT, index=df.index)
    dates = _dt_delivery.combine_first(_dt_request)
    valid = dates.notna()
    df2   = df[valid].copy()
    df2['_dt'] = dates[valid].values
    df_new = df2[df2['_dt'] > last_date]

    if df_new.empty:
        print(f"  AZ Returns: 0 new rows (last={last_date_str})")
        return {}, last_date_str, {}, {}, []

    reasons = {}
    az_return_reason_index = {}
    az_order_awb_index     = {}
    return_rows = []
    for _, row in df_new.iterrows():
        oid = str(row.get(oid_col, '') or '').strip()
        if not oid or oid.lower() == 'nan':
            continue
        reason = str(row.get(reason_col, '') or '').strip() if reason_col else ''
        track  = str(row.get(track_col, '') or '').strip() if track_col else ''
        sku    = str(row.get(sku_col, '') or '').strip() if sku_col else ''
        if reason and reason.lower() not in ('nan', ''):
            reasons[reason] = reasons.get(reason, 0) + 1
            az_return_reason_index[oid] = reason
        if track and track.lower() not in ('nan', ''):
            az_order_awb_index[oid] = track
        return_rows.append({
            'order_id': oid, 'return_date': row['_dt'].isoformat(),
            'return_reason': reason, 'tracking_id': track,
            'sku': sku if sku.lower() not in ('nan', '') else '',
        })

    new_last = df_new['_dt'].max()
    print(f"  AZ Returns: {len(df_new)} new rows ({last_date_str} -> {new_last.isoformat()})")
    return reasons, new_last.isoformat(), az_return_reason_index, az_order_awb_index, return_rows


def build_az_ledger_rows(az_order_rows, az_settlement_fees, az_return_reason_index,
                          return_receipts, packaging_config, az_order_awb_index=None):
    """
    Builds Orders Ledger rows for Amazon (self-ship/MFN) — mirrors
    build_fk_ledger_rows/build_me_ledger_rows exactly.

    az_order_rows:          per-order dicts from process_az_orders_report
    az_settlement_fees:     {order_id: {settlement, commission, shipping_fwd,
                            fixed_fee}} from process_az_settlement_report
    az_return_reason_index: {order_id: reason_str} from process_az_returns_report
    return_receipts:        from sheets_connector.fetch_return_receipts()
    packaging_config:       dict with packaging_cost_per_order, bubble_wrap_cost, bubble_wrap_cutoff
    az_order_awb_index:     {order_id: tracking_id} from process_az_returns_report

    is_returned is determined from az_return_reason_index (did the Returns
    report report a return for this order_id) rather than an order-status
    column — that's the one ground-truth signal confirmed reliable from the
    docs for a self-ship order (see process_az_orders_report's status note).
    No Amazon claims data source exists yet — claim_id/claim_status/
    claim_recovered are always blank/0 for now.
    """
    az_order_awb_index = az_order_awb_index or {}
    if not az_order_rows:
        return []

    pkg_cost         = float(packaging_config.get('packaging_cost_per_order', 12.0))
    bubble_cutoff    = packaging_config.get('bubble_wrap_cutoff', '2026-05-01')
    always_lost_cost = float(packaging_config.get('always_lost_cost', 0.0))
    box_sticker_cost = float(packaging_config.get('box_sticker_cost', 0.0))
    chain_cost       = float(packaging_config.get('chain_cost', 0.0))

    ledger_rows = []
    for row in az_order_rows:
        oid  = row.get('order_id', '')
        dt   = row.get('order_date', '')
        cogs = float(row.get('cogs', 0) or 0)

        receipt = return_receipts.get(oid) or return_receipts.get(az_order_awb_index.get(oid, '')) or {}
        earring_cond = receipt.get('earring_condition', '')
        box_cond     = receipt.get('box_condition', '')
        chain_cond   = receipt.get('chain_condition', '')

        return_reason = az_return_reason_index.get(oid, '')
        is_returned   = oid in az_return_reason_index

        fees         = az_settlement_fees.get(oid, {})
        sett         = float(fees.get('settlement', 0) or 0)
        commission   = float(fees.get('commission', 0) or 0)
        shipping_fwd = float(fees.get('shipping_fwd', 0) or 0)
        tcs          = float(fees.get('tcs', 0) or 0)
        fixed_fee    = float(fees.get('fixed_fee', 0) or 0)

        eff_pkg_cost = pkg_cost
        if dt < bubble_cutoff:
            eff_pkg_cost += float(packaging_config.get('bubble_wrap_cost', 2.0))

        return_loss_value = cogs if (is_returned and earring_cond == 'Damaged') else 0.0
        packaging_loss = (
            always_lost_cost + (box_sticker_cost if box_cond == 'Damaged' else 0.0)
        ) if is_returned else 0.0
        chain_loss = chain_cost if (is_returned and chain_cond == 'Damaged') else 0.0

        net_pl = round(
            sett - cogs - eff_pkg_cost
            - return_loss_value - packaging_loss - chain_loss,
            2
        )

        matched_order_id = oid if receipt else ''
        return_pl = round(-return_loss_value - packaging_loss - chain_loss, 2) if is_returned else 0.0

        ledger_rows.append({
            **row,
            'cogs':              round(cogs, 2),
            'settlement':        round(sett, 2),
            'commission':        round(commission, 2),
            'shipping_fwd':      round(shipping_fwd, 2),
            'tcs':               round(tcs, 2),
            'fixed_fee':         round(fixed_fee, 2),
            # Never populated for Amazon (no data source for these yet) —
            # set explicitly rather than relying on **row, so a row reloaded
            # from the persisted az_orders_daily table (which only carries
            # the static order facts, not these) behaves identically to a
            # freshly-parsed one instead of silently blanking out cells that
            # were previously written to the live Ledger sheet.
            'collection_fee':    0.0,
            'shipping_rev':      0.0,
            'gst_on_fees':       0.0,
            'tds':               0.0,
            'penalty':           0.0,
            'packaging_cost':    round(eff_pkg_cost, 2),
            'ad_spend_apport':   0.0,
            'status':            'Returned-Customer' if is_returned else 'Delivered',
            'return_reason':     return_reason,
            'earring_condition': earring_cond,
            'box_condition':     box_cond,
            'chain_condition':   chain_cond,
            'return_loss_value': round(return_loss_value, 2),
            'packaging_loss':    round(packaging_loss, 2),
            'chain_loss':        round(chain_loss, 2),
            'claim_id':          '',
            'claim_status':      'not_raised',
            'claim_recovered':   0.0,
            'net_pl':            net_pl,
            'matched_order_id':  matched_order_id,
            'return_pl':         return_pl,
        })

    return ledger_rows


def process_az_search_query_performance(json_text):
    """
    Parses a Search Query Performance report document into flat row dicts,
    one per (asin, search_query, period). Field names match Amazon's own
    schema verbatim (github.com/amzn/selling-partner-api-models
    schemas/reports/sellingPartnerSearchQueryPerformanceReport.json,
    confirmed 2026-07-15 -- not guessed from summarized docs). Unlike
    Orders/Settlement/Returns, this report's document is JSON, not a
    flat/tab-delimited file.

    Deliberately excludes the same-day/1-day/2-day shipping-speed
    breakdowns Amazon also provides under clickData/cartAddData/
    purchaseData (totalSameDayShippingClickCount and its siblings) --
    out of scope for v1. The core funnel (impressions -> clicks ->
    cart adds -> purchases, each as total/asin-specific/share) is what
    FK/ME's own keyword tracking surfaces today; the shipping-speed
    slice can be added later as extra columns without changing this
    row shape, if it turns out to matter.

    Returns: list of row dicts.
    """
    data = json.loads(json_text)
    period_type = ((data.get('reportSpecification') or {}).get('reportOptions') or {}).get('reportPeriod', '')

    rows = []
    for entry in data.get('dataByAsin', []):
        asin = str(entry.get('asin', '')).strip()
        sq   = entry.get('searchQueryData')  or {}
        imp  = entry.get('impressionData')   or {}
        clk  = entry.get('clickData')        or {}
        cart = entry.get('cartAddData')      or {}
        pur  = entry.get('purchaseData')     or {}

        query = str(sq.get('searchQuery', '')).strip()
        if not asin or not query:
            continue

        rows.append({
            'period_type':          period_type,
            'period_start':         entry.get('startDate', ''),
            'period_end':           entry.get('endDate', ''),
            'asin':                 asin,
            'search_query':         query,
            'search_query_score':   sq.get('searchQueryScore', ''),
            'search_query_volume':  sq.get('searchQueryVolume', ''),
            'impressions_total':    imp.get('totalQueryImpressionCount', 0),
            'impressions_asin':     imp.get('asinImpressionCount', 0),
            'impressions_share':    imp.get('asinImpressionShare', 0),
            'clicks_total':         clk.get('totalClickCount', 0),
            'clicks_total_rate':    clk.get('totalClickRate', 0),
            'clicks_asin':          clk.get('asinClickCount', 0),
            'clicks_asin_share':    clk.get('asinClickShare', 0),
            'cart_adds_total':      cart.get('totalCartAddCount', 0),
            'cart_adds_total_rate': cart.get('totalCartAddRate', 0),
            'cart_adds_asin':       cart.get('asinCartAddCount', 0),
            'cart_adds_asin_share': cart.get('asinCartAddShare', 0),
            'purchases_total':      pur.get('totalPurchaseCount', 0),
            'purchases_total_rate': pur.get('totalPurchaseRate', 0),
            'purchases_asin':       pur.get('asinPurchaseCount', 0),
            'purchases_asin_share': pur.get('asinPurchaseShare', 0),
        })

    print(f"  AZ SQP: {len(rows)} (asin, query) rows parsed")
    return rows


def _az_request_report(db, kind):
    """
    Fires a NEW Amazon Orders/Returns report request (kind = 'orders' or
    'returns') if none is currently pending and the watermark says one is
    due. Deliberately does NOT poll or download -- that's _az_poll_report,
    called later in the same run after Flipkart/Meesho file processing, so
    that processing time (several minutes) doubles as Amazon's real-world
    report-preparation wait instead of that wait going unused (2026-07-15,
    Jaiswal -- live test showed reports ready in ~7 min, about the same as
    FK/ME processing takes).

    State persists across runs via db['config'], same pattern as every
    other platform's *_last_date watermark:
      az_{kind}_pending_report_id — set while waiting on Amazon
      az_{kind}_pending_end       — the requested range's end date, used to
                                    advance the watermark once the report
                                    lands (the report is authoritative for
                                    its whole requested window, so this is
                                    used instead of the max date actually
                                    found in the content — a day with zero
                                    orders is real, not a gap)
      az_{kind}_last_date         — the watermark itself

    Returns {'status', 'warnings', 'errors'} -- status one of
    'requested' / 'already_pending' / 'up_to_date' / 'failed'.
    """
    from datetime import timedelta
    import amazon_connector as az

    report_type = {'orders': az.REPORT_TYPE_ORDERS, 'returns': az.REPORT_TYPE_RETURNS}[kind]
    pending_key   = f'az_{kind}_pending_report_id'
    end_key       = f'az_{kind}_pending_end'
    watermark_key = f'az_{kind}_last_date'

    result = {'status': 'skipped', 'warnings': [], 'errors': []}

    if get_config(db, pending_key, ''):
        # A report is already awaited (from this run's -- impossible, this
        # runs once -- or a previous run's request); _az_poll_report handles
        # checking it. Never fire a second request while one is outstanding.
        result['status'] = 'already_pending'
        return result

    watermark_str = get_config(db, watermark_key, '2026-01-01')
    try:
        watermark_date = (datetime.strptime(watermark_str, '%Y-%m-%d').date()
                          if watermark_str != '1970-01-01' else date(2026, 1, 1))
    except ValueError as e:
        # Fail safe rather than let a malformed config value crash the whole
        # pipeline run (dashboard memory active.md #57 review finding).
        result['errors'].append(f"AZ {kind}: malformed watermark {watermark_str!r} — {e}")
        result['status'] = 'failed'
        return result
    yesterday = date.today() - timedelta(days=1)
    if watermark_date >= yesterday:
        result['status'] = 'up_to_date'
        return result

    range_end = min(yesterday, watermark_date + timedelta(days=30))   # createReport's own 30-day max span
    try:
        report_id = az.create_report(
            report_type,
            data_start_time=watermark_date.isoformat() + 'T00:00:00Z',
            data_end_time=range_end.isoformat() + 'T23:59:59Z',
        )
    except Exception as e:
        result['errors'].append(f"AZ {kind}: createReport failed — {e}")
        result['status'] = 'failed'
        return result

    set_config(db, pending_key, f'id_{report_id}')
    set_config(db, end_key, range_end.isoformat())
    result['status'] = 'requested'
    print(f"  AZ {kind}: requested report {report_id} for {watermark_date} -> {range_end}")
    return result


def _az_poll_report(db, kind):
    """
    Checks/downloads whatever Amazon Orders/Returns report is currently
    pending -- whether requested by _az_request_report earlier in this same
    run, or carried over from a previous run. Companion to
    _az_request_report (see its docstring for why these are split).

    Returns {'status', 'content', 'range_end', 'warnings', 'errors'}.
    'content' is the downloaded report text only if a report completed THIS
    run; otherwise None (still pending or nothing pending — caller checks
    'status'). When 'content' is set, 'range_end' (str) is the date to
    advance the watermark to.
    """
    import amazon_connector as az

    pending_key = f'az_{kind}_pending_report_id'
    end_key     = f'az_{kind}_pending_end'

    result = {'status': 'skipped', 'content': None, 'range_end': None, 'warnings': [], 'errors': []}

    # Stored/read with a non-numeric prefix -- load_db()'s CSV round-trip
    # auto-converts purely-numeric config VALUES to float (needed elsewhere
    # for real numeric settings), which silently corrupted a raw Amazon
    # report ID like "50244020648" into "50244020648.0" on the very next
    # run, and Amazon correctly rejected that as an invalid id (confirmed
    # via a real 404 in production, 2026-07-14). The "id_" prefix keeps the
    # stored string non-numeric so it survives the round-trip unmodified.
    raw_pending = get_config(db, pending_key, '')
    pending_id = raw_pending[3:] if raw_pending.startswith('id_') else raw_pending
    if not pending_id:
        result['status'] = 'up_to_date'
        return result

    try:
        info = az.get_report(pending_id)
    except Exception as e:
        result['errors'].append(f"AZ {kind}: could not poll report {pending_id} — {e}")
        result['status'] = 'failed'
        # A 404/"not a valid Id" means the stored id itself is bad (e.g.
        # the numeric-corruption bug this fix addresses) -- clear it so
        # a fresh, correctly-formed request goes out next run instead of
        # retrying the same broken id forever. Any other error (network,
        # timeout, 5xx) is likely transient -- leave the marker so the
        # SAME still-valid report gets polled again next run.
        if '404' in str(e) or 'not a valid Id' in str(e):
            set_config(db, pending_key, '')
        return result

    status = info.get('processingStatus')
    if status not in az.TERMINAL_STATUSES:
        result['status'] = 'pending'
        print(f"  AZ {kind}: report {pending_id} still {status} — will check again next run")
        return result

    if status == 'DONE':
        doc_id = info.get('reportDocumentId')
        try:
            result['content'] = az.get_report_document(doc_id)
        except Exception as e:
            result['errors'].append(f"AZ {kind}: report {pending_id} DONE but download failed — {e}")
            result['status'] = 'failed'
            return result
        result['status']    = 'ok'
        result['range_end'] = get_config(db, end_key, '')
        set_config(db, pending_key, '')
        return result

    # CANCELLED or FATAL — clear the marker so a fresh request goes out next run
    result['errors'].append(f"AZ {kind}: report {pending_id} ended {status} — will re-request next run")
    result['status'] = 'failed'
    set_config(db, pending_key, '')
    return result


def _az_acquire_settlement(db):
    """
    Settlement reports are auto-scheduled by Amazon, not requestable on
    demand — only discoverable via getReports (dashboard memory #57).
    Downloads every new DONE report since the last one processed (a run
    might see several at once if there's a backlog).

    Returns: {'status', 'contents': [(created_time, content_str), ...],
    'warnings', 'errors'}. Deliberately does NOT advance
    az_settlement_last_created itself — a report that downloads fine here
    but fails to PARSE later (main() calls process_az_settlement_report)
    must not be treated as processed, or it's silently skipped forever.
    The caller advances the watermark only past reports it actually
    parses successfully (dashboard memory active.md #57 review finding).
    """
    import amazon_connector as az

    result = {'status': 'skipped', 'contents': [], 'warnings': [], 'errors': []}
    last_created = get_config(db, 'az_settlement_last_created', '')

    try:
        reports = az.list_reports([az.REPORT_TYPE_SETTLEMENT], created_since=last_created or None)
    except Exception as e:
        result['errors'].append(f"AZ settlement: listReports failed — {e}")
        result['status'] = 'failed'
        return result

    done = [r for r in reports if r.get('processingStatus') == 'DONE' and r.get('reportDocumentId')]
    done.sort(key=lambda r: r.get('createdTime', ''))   # oldest first so the watermark advances correctly

    if not done:
        result['status'] = 'up_to_date'
        return result

    for r in done:
        try:
            content = az.get_report_document(r['reportDocumentId'])
        except Exception as e:
            result['errors'].append(f"AZ settlement: report {r.get('reportId')} download failed — {e}")
            continue
        result['contents'].append((r.get('createdTime', ''), content))

    result['status'] = 'ok' if result['contents'] else 'failed'
    return result


def _az_get_active_asins():
    """
    Returns (asins, error) -- a sorted list of distinct ASINs from the latest
    rumee_az_catalog Firestore doc (same source process_az_catalog_for_pm()
    already reads for Product Master), plus an error string if the read
    itself failed. Kept distinct from "genuinely zero ASINs in the catalog"
    (error=None, asins=[]) so a real Firestore/connectivity failure surfaces
    into _run_errors/the Discord notification (Golden Rule 29 -- no silent
    errors) instead of being indistinguishable from an empty catalog.
    """
    try:
        from firestore_connector import get_db
        fdb = get_db()
        docs = list(fdb.collection('rumee_az_catalog').stream())
        if not docs:
            return [], None
        latest = max(docs, key=lambda d: d.id)
        rows = (latest.to_dict() or {}).get('rows', [])
    except Exception as e:
        return [], f"AZ SQP: catalog read error — {e}"

    asins = set()
    for row in rows:
        asin1 = str(row.get('asin1', '')).strip()
        if asin1 and asin1.lower() != 'nan':
            asins.add(asin1)
    return sorted(asins), None


def _az_chunk_asins(asins, max_len=200):
    """
    Groups ASINs into space-separated batches no longer than max_len chars
    -- Amazon's own reportOptions.asin limit for Search Query Performance
    (confirmed against the live SP-API docs, 2026-07-15: "There is a 200
    character limit"). One createReport call is fired per chunk.
    """
    chunks, current = [], []
    for a in asins:
        candidate = current + [a]
        if len(' '.join(candidate)) > max_len and current:
            chunks.append(current)
            current = [a]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _az_request_sqp(db):
    """
    Fires new Search Query Performance report requests for the most recent
    FULLY COMPLETE calendar week. Amazon's own alignment rule (confirmed
    live docs, 2026-07-15): reportPeriod=WEEK requires dataStartTime to be
    a Sunday and dataEndTime the following Saturday, and "requests cannot
    span multiple periods" -- so this always requests exactly one
    Sunday-to-Saturday week, never a rolling window like Orders/Returns.

    Our ASIN catalog is chunked into <=200-char groups (_az_chunk_asins)
    since a single request's reportOptions.asin can't hold more -- one
    createReport call per chunk.

    Same create-then-poll-later split as _az_request_report/_az_poll_report
    (Amazon's real report-prep wait overlaps FK/ME processing below). Unlike
    those functions' single pending-id, this tracks a LIST of pending
    report ids for the whole week -- see _az_poll_sqp for why (partial
    per-chunk failure must not silently skip data or block forever).

    Returns {'status', 'warnings', 'errors'} -- status one of
    'requested' / 'already_pending' / 'up_to_date' / 'no_asins' / 'failed'.
    """
    from datetime import timedelta
    import amazon_connector as az

    result = {'status': 'skipped', 'warnings': [], 'errors': []}

    if get_config(db, 'az_sqp_week_start', ''):
        # A week is already in flight -- _az_poll_sqp resolves it (fully or
        # partially) before this fires the next one. Never overlap weeks.
        result['status'] = 'already_pending'
        return result

    yesterday = date.today() - timedelta(days=1)   # today's own week isn't final yet
    days_since_saturday = (yesterday.weekday() - 5) % 7   # weekday(): Mon=0 .. Sat=5, Sun=6
    week_end   = yesterday - timedelta(days=days_since_saturday)   # most recent complete Saturday
    week_start = week_end - timedelta(days=6)                       # its Sunday

    watermark = get_config(db, 'az_sqp_last_week_end', '')
    if watermark and watermark >= week_end.isoformat():
        result['status'] = 'up_to_date'
        return result

    asins, asins_error = _az_get_active_asins()
    if asins_error:
        result['errors'].append(asins_error)
        result['status'] = 'failed'
        return result
    if not asins:
        result['warnings'].append(
            "AZ SQP: no ASINs found in rumee_az_catalog -- catalog data still "
            "pending validation (item #17), skipping this run")
        result['status'] = 'no_asins'
        return result

    chunks = _az_chunk_asins(asins)
    pending_ids = []
    for chunk in chunks:
        try:
            report_id = az.create_report(
                az.REPORT_TYPE_SEARCH_QUERY_PERFORMANCE,
                data_start_time=week_start.isoformat() + 'T00:00:00Z',
                data_end_time=week_end.isoformat() + 'T23:59:59Z',
                report_options={'reportPeriod': 'WEEK', 'asin': ' '.join(chunk)},
            )
            pending_ids.append(report_id)
        except Exception as e:
            result['errors'].append(f"AZ SQP: createReport failed for a {len(chunk)}-ASIN chunk — {e}")

    if not pending_ids:
        # Every chunk failed -- don't persist any in-flight state, so this
        # same week is retried fresh next run instead of getting stuck.
        result['status'] = 'failed'
        return result

    set_config(db, 'az_sqp_week_start', week_start.isoformat())
    set_config(db, 'az_sqp_week_end', week_end.isoformat())
    set_config(db, 'az_sqp_pending', json.dumps(pending_ids))
    result['status'] = 'requested'
    print(f"  AZ SQP: requested {len(pending_ids)}/{len(chunks)} chunk(s) for week {week_start} -> {week_end} ({len(asins)} ASINs)")
    return result


def _az_poll_sqp(db):
    """
    Checks every currently-pending Search Query Performance report id.
    Returns the raw JSON content for each DONE report (caller parses via
    process_az_search_query_performance). A chunk that ends FATAL/CANCELLED
    is dropped from the pending list with a warning -- that ASIN slice is
    genuinely missing for this week -- rather than blocking the whole week
    forever. The watermark only advances once the pending list is fully
    drained (every chunk resolved, successfully or not): partial data for a
    week is accepted and flagged, never silently treated as complete, and
    never retried indefinitely on a permanently-failed chunk.

    Returns {'status', 'contents': [str, ...], 'warnings', 'errors'}.
    """
    import amazon_connector as az

    result = {'status': 'skipped', 'contents': [], 'warnings': [], 'errors': []}

    week_start = get_config(db, 'az_sqp_week_start', '')
    week_end   = get_config(db, 'az_sqp_week_end', '')
    if not week_start:
        result['status'] = 'up_to_date'
        return result

    raw_pending = get_config(db, 'az_sqp_pending', '[]')
    try:
        pending_ids = json.loads(raw_pending) if raw_pending else []
    except (ValueError, TypeError):
        pending_ids = []

    still_pending = []
    for report_id in pending_ids:
        try:
            info = az.get_report(report_id)
        except Exception as e:
            result['errors'].append(f"AZ SQP: could not poll report {report_id} — {e}")
            # A 404/"not a valid Id" means the stored id itself is bad (same
            # class of bug _az_poll_report already guards against) -- drop it
            # rather than retrying a permanently-invalid id forever. Any
            # other error (network, timeout, 5xx) is likely transient --
            # keep it in still_pending so the same valid report is re-checked
            # next run.
            if '404' not in str(e) and 'not a valid Id' not in str(e):
                still_pending.append(report_id)
            continue

        status = info.get('processingStatus')
        if status not in az.TERMINAL_STATUSES:
            still_pending.append(report_id)
            continue

        if status == 'DONE':
            doc_id = info.get('reportDocumentId')
            try:
                result['contents'].append(az.get_report_document(doc_id))
            except Exception as e:
                result['errors'].append(f"AZ SQP: report {report_id} DONE but download failed — {e}")
                still_pending.append(report_id)   # download-only retry next run, id still valid
            continue

        # CANCELLED or FATAL -- this chunk's data is genuinely lost for this
        # week; don't retry a permanently-failed id forever.
        result['warnings'].append(
            f"AZ SQP: chunk report {report_id} ended {status} — that ASIN "
            f"slice is missing for week {week_start}..{week_end}")

    if still_pending:
        set_config(db, 'az_sqp_pending', json.dumps(still_pending))
        result['status'] = 'partial' if result['contents'] else 'pending'
        print(f"  AZ SQP: {len(still_pending)} chunk(s) for week {week_start} -> {week_end} "
              f"still processing — will check again next run")
        return result

    # Every chunk resolved (downloaded or permanently failed) -- close out
    # the week and advance the watermark regardless, so a chunk Amazon will
    # never deliver doesn't stall this report forever.
    set_config(db, 'az_sqp_last_week_end', week_end)
    set_config(db, 'az_sqp_week_start', '')
    set_config(db, 'az_sqp_week_end', '')
    set_config(db, 'az_sqp_pending', '[]')
    result['status'] = 'ok' if result['contents'] else 'no_data'
    print(f"  AZ SQP: week {week_start} -> {week_end} closed, {len(result['contents'])} chunk(s) downloaded")
    return result


def _az_request_catalog(db):
    """
    Fires a new GET_MERCHANT_LISTINGS_ALL_DATA request -- a full-catalog
    snapshot, not incremental (confirmed against the live SP-API "Report
    Type Values" docs, 2026-07-17: only optional dataStartTime/marketplaceIds,
    no end date). Only re-requests if nothing is currently pending AND the
    last successful pull was 7+ days ago -- the catalog doesn't change often
    enough to justify generating this (large) report every single run.

    Replaces the one-off push_az_catalog_firestore.py script (2026-06-30,
    dashboard memory active.md item #17) with a regular pipeline pull.

    Returns {'status', 'warnings', 'errors'} -- status one of
    'requested' / 'already_pending' / 'up_to_date' / 'failed'.
    """
    from datetime import timedelta
    import amazon_connector as az

    result = {'status': 'skipped', 'warnings': [], 'errors': []}

    if get_config(db, 'az_catalog_pending_report_id', ''):
        result['status'] = 'already_pending'
        return result

    last_pulled = get_config(db, 'az_catalog_last_pulled', '')
    if last_pulled:
        try:
            days_since = (date.today() - datetime.strptime(last_pulled, '%Y-%m-%d').date()).days
            if days_since < 7:
                result['status'] = 'up_to_date'
                return result
        except ValueError:
            pass   # malformed watermark -- fall through and re-request

    try:
        report_id = az.create_report(az.REPORT_TYPE_MERCHANT_LISTINGS)
    except Exception as e:
        result['errors'].append(f"AZ catalog: createReport failed — {e}")
        result['status'] = 'failed'
        return result

    # 'id_' prefix survives the CSV round-trip unmodified -- same fix as
    # _az_poll_report's pending_key (dashboard memory active.md #57: a raw
    # numeric report id gets silently corrupted to "N.0" by load_db()'s
    # auto-float-conversion otherwise).
    set_config(db, 'az_catalog_pending_report_id', 'id_' + str(report_id))
    result['status'] = 'requested'
    print(f"  AZ catalog: requested full-listings snapshot (report {report_id})")
    return result


def _az_poll_catalog(db):
    """
    Checks the pending GET_MERCHANT_LISTINGS_ALL_DATA report, if any.
    Mirrors _az_poll_report's single-pending-id pattern (catalog is one
    report, not chunked like SQP).

    Returns {'status', 'content', 'warnings', 'errors'}. 'content' is the
    downloaded report text only if it completed THIS run.
    """
    import amazon_connector as az

    result = {'status': 'skipped', 'content': None, 'warnings': [], 'errors': []}

    raw_pending = get_config(db, 'az_catalog_pending_report_id', '')
    pending_id = raw_pending[3:] if raw_pending.startswith('id_') else raw_pending
    if not pending_id:
        result['status'] = 'up_to_date'
        return result

    try:
        info = az.get_report(pending_id)
    except Exception as e:
        result['errors'].append(f"AZ catalog: could not poll report {pending_id} — {e}")
        result['status'] = 'failed'
        if '404' in str(e) or 'not a valid Id' in str(e):
            set_config(db, 'az_catalog_pending_report_id', '')
        return result

    status = info.get('processingStatus')
    if status not in az.TERMINAL_STATUSES:
        result['status'] = 'pending'
        print(f"  AZ catalog: report {pending_id} still {status} — will check again next run")
        return result

    if status == 'DONE':
        doc_id = info.get('reportDocumentId')
        try:
            result['content'] = az.get_report_document(doc_id)
        except Exception as e:
            result['errors'].append(f"AZ catalog: report {pending_id} DONE but download failed — {e}")
            result['status'] = 'failed'
            return result
        result['status'] = 'ok'
        set_config(db, 'az_catalog_pending_report_id', '')
        return result

    # CANCELLED or FATAL — clear the marker so a fresh request goes out next run
    result['errors'].append(f"AZ catalog: report {pending_id} ended {status} — will re-request next run")
    result['status'] = 'failed'
    set_config(db, 'az_catalog_pending_report_id', '')
    return result


def process_az_catalog_report(content):
    """
    Parses GET_MERCHANT_LISTINGS_ALL_DATA (tab-separated). Column names are
    preserved EXACTLY as Amazon sends them (item-name, seller-sku, asin1,
    status, ...) -- no lowercasing/renaming -- so the pushed Firestore rows
    have the identical shape the one-off az_catalog_2026-06-30.csv push used,
    and _az_get_active_asins()'s row.get('asin1') keeps working unchanged.

    Returns a list of row dicts (NaN cells -> '').
    """
    df = pd.read_csv(io.StringIO(content), sep='\t', dtype=str, on_bad_lines='skip')
    if df.empty:
        return []
    return df.fillna('').to_dict('records')


# ─── Sale-triggered stock decrement + return credit-back (dashboard memory ──
# active.md item #64, 2026-07-17). Jaiswal: "when an order is placed... we
# are for sure shipping that... that's going to decrease the stock... when
# we receive return or RTO... increase the stock if intact."

def _resolve_order_sku(platform, raw_sku, pm_sku_index, sku_overrides):
    """
    Resolves one order-line's raw SKU string to a product_master doc id.
    Checks manual overrides first (real naming drift confirmed in live data,
    e.g. "Bahubali DJ7" vs "DJ-7 Bahubali" -- exact-match only, this never
    fuzzy-matches or guesses, per Jaiswal's explicit instruction), then the
    live product_master listing index. Returns (product_master_id, None) on
    success, (None, normalized_raw_sku) on failure so the caller can log it
    to rumee_stock_unresolved instead of silently skipping.
    """
    norm = str(raw_sku or '').strip().lower()
    if not norm:
        return None, None
    key = (platform, norm)
    if key in sku_overrides:
        return sku_overrides[key], None
    if key in pm_sku_index:
        return pm_sku_index[key], None
    return None, norm


def _process_sale_stock_decrement(fk_orders_sku_new, me_order_rows_new, az_orders_new):
    """
    Resolves each NEW-this-run order row (per platform) to a real Product
    Master item, walks that item's final BOM (rumee_boms, output_type=
    'final'), decrements each ingredient's Item Master stock.

    Per-platform join field, confirmed against real live Firestore data
    (2026-07-17), not guessed -- differs per platform:
      Flipkart: fk_orders_sku.sku       (already matches listings[].sku_id)
      Meesho:   me_order_rows.sku_name  (NOT sku, which is a short internal
                                          code, e.g. "dj11-me")
      Amazon:   az_orders_daily.sku     (already matches listings[].sku_id)

    Quantity field per platform:
      Flipkart: fk_orders_sku.quantity  -- ASSUMPTION, flagged, not fully
                verified: process_fk_orders's docstring/columns show no
                status field at all, suggesting Flipkart's Fulfilment
                Orders report (unlike Meesho's raw order export) may not
                carry cancelled orders in the first place. If this proves
                wrong, the fix is the same shape as the Meesho one below --
                exclude cancelled rows before summing quantity.
      Meesho:   me_order_rows.qty, EXCLUDING status == 'Cancelled' (maps
                from raw CANCELLED/LOST via _ME_STATUS_MAP) -- confirmed via
                a real raw ME order row shared 2026-07-17 with status
                'SHIPPED' that an aggregate field summing every status
                regardless (me_daily.total_units) would have wrongly
                included.
      Amazon:   az_orders_daily.qty     -- already per-order, no aggregate-
                across-statuses concern.

    Idempotency -- IMPORTANT, this was a real bug caught by an independent
    review before shipping (2026-07-17): this function MUST only ever see
    genuinely new-this-run rows per platform, since nothing here re-checks
    against already-applied movements. fk_orders_sku_new/az_orders_new both
    come from row-level-watermarked sources (process_fk_orders/
    process_az_orders_report, same "new rows this run" guarantee the Orders
    Ledger already relies on) -- safe. The Meesho parameter MUST be
    me_order_rows (from process_meesho_orders, also row-level watermarked)
    -- NOT me_daily_new/build_me_daily's output, which recomputes a rolling
    6-month window from whatever raw files were downloaded this run with NO
    watermark of its own (confirmed by reading build_me_daily itself). Since
    Meesho's own order-export files are rolling multi-day windows (that's
    exactly why process_meesho_orders needs its own row-level watermark on
    top of Drive's file-level tracking), passing me_daily_new here would
    silently re-decrement the same real sales on every subsequent pipeline
    run -- caught before it ever ran against live data.

    Unresolved SKUs go to rumee_stock_unresolved for the dashboard's
    mapping UI -- never guessed/fuzzy-matched (Jaiswal: "it should not
    happen as all sales carry sku id which is linked in product master" --
    treated as a real gap to fix via mapping, not routine/expected).

    Returns a summary dict for Discord visibility (Golden Rule 29).
    """
    from firestore_connector import (load_product_master_sku_index, load_stock_sku_overrides,
                                      load_final_boms, apply_stock_movements, write_stock_unresolved)

    summary = {'resolved': 0, 'unresolved': 0, 'movements': 0, 'no_bom': 0, 'errors': []}
    if not (fk_orders_sku_new or me_order_rows_new or az_orders_new):
        return summary

    try:
        pm_sku_index  = load_product_master_sku_index()
        sku_overrides = load_stock_sku_overrides()
        final_boms    = load_final_boms()
    except Exception as e:
        summary['errors'].append(f"Stock decrement: could not load resolution data — {e}")
        return summary

    movements = []
    unresolved_entries = []

    def _handle(platform, raw_sku, qty, date_str, source_id):
        if qty <= 0:
            return
        pm_id, unresolved_key = _resolve_order_sku(platform, raw_sku, pm_sku_index, sku_overrides)
        if not pm_id:
            unresolved_entries.append({'platform': platform, 'raw_sku': raw_sku, 'date': date_str, 'qty': qty})
            summary['unresolved'] += 1
            return
        bom = final_boms.get(pm_id)
        if not bom or not bom.get('components'):
            # Resolved to a real product, just no BOM defined for it yet --
            # a DIFFERENT gap than an unresolved SKU (Jaiswal is going
            # through Product Master building these one at a time), so this
            # does NOT go into rumee_stock_unresolved -- that queue is
            # specifically for SKU-matching gaps, not missing-BOM gaps.
            summary['no_bom'] += 1
            return
        summary['resolved'] += 1
        for c in bom['components']:
            qty_needed = float(c.get('qty_per_unit', 0) or 0) * qty
            if qty_needed <= 0:
                continue
            movements.append({
                'material_id': c['material_id'], 'direction': 'out', 'qty': qty_needed,
                'source_type': 'sale', 'source_id': source_id, 'date': date_str,
                'notes': f"Sale: {platform} {raw_sku} x{qty}",
            })

    for r in (fk_orders_sku_new or []):
        _handle('flipkart', r.get('sku'), float(r.get('quantity', 0) or 0), r.get('date'),
                f"fk_{r.get('date')}_{r.get('sku')}")

    for r in (me_order_rows_new or []):
        if r.get('status') == 'Cancelled':
            continue
        _handle('meesho', r.get('sku_name'), float(r.get('qty', 0) or 0), r.get('order_date'),
                f"me_{r.get('order_id')}")

    for r in (az_orders_new or []):
        _handle('amazon', r.get('sku'), float(r.get('qty', 0) or 0), r.get('order_date'),
                f"az_{r.get('order_id')}")

    if movements:
        try:
            apply_stock_movements(movements)
            summary['movements'] = len(movements)
        except Exception as e:
            summary['errors'].append(f"Stock decrement: apply_stock_movements failed — {e}")

    if unresolved_entries:
        try:
            write_stock_unresolved(unresolved_entries)
        except Exception as e:
            summary['errors'].append(f"Stock decrement: write_stock_unresolved failed — {e}")

    print(f"  Stock decrement: {summary['resolved']} order-line(s) resolved ({summary['movements']} material "
          f"movement(s)), {summary['no_bom']} resolved-but-no-BOM, {summary['unresolved']} unresolved SKU(s)")
    return summary


def _process_return_stock_credit(existing_return_credits, fk_order_sku_index_rows,
                                  me_order_sku_index_rows, az_orders_daily_rows,
                                  fk_order_awb_index_rows, me_order_awb_index_rows,
                                  az_returns_daily_rows,
                                  pm_sku_index, sku_overrides, final_boms):
    """
    Credits Item Master stock back for returns whose scanned condition
    marks a component as intact (Jaiswal, explicit: per-component -- earring
    /box/chain credited independently based on their own condition column,
    not a single all-or-nothing gate).

    Reads fetch_return_receipts() fresh (always the FULL current sheet
    state, not a delta -- unlike order data, this DOES need its own
    idempotency: existing_return_credits, one row per order_id, one flag per
    component already credited).

    Order -> SKU resolution uses the FULL persisted history for each
    platform (fk_order_sku_index_rows / me_order_sku_index_rows /
    az_orders_daily_rows, not just this run's new rows) -- a return is
    typically scanned days or weeks after its order, long after that
    order's own file-processing run has finished.

    Receipt lookup direction matches the existing Ledger builders exactly
    (build_fk_ledger_rows/build_me_ledger_rows/build_az_ledger_rows,
    receipts.get(oid) or receipts.get(awb_index.get(oid, ''))) -- the
    Returns Scanner records the AWB, and may not have captured order_id/
    suborder number, so receipts can be keyed by either. We iterate over
    orders we KNOW about (from the sku indices) and probe receipts by
    order_id first, then that order's own AWB (from
    fk_order_awb_index_rows/me_order_awb_index_rows/az_returns_daily_rows'
    own per-return tracking_id) as a fallback -- never the reverse, since
    iterating receipts.items() directly would treat every receipt key as an
    order_id even when it's actually an AWB, silently dropping AWB-only
    receipts (a real gap caught before this shipped, not a hypothetical).

    Component -> BOM ingredient matching is by Material `type` (the only
    generic signal available -- BOM ingredient names are arbitrary, never
    tagged as "the chain" or "the box" specifically):
      earring_condition -> ingredients where material.type == 'base_earring'
      box_condition     -> ingredients where material.type == 'packaging'
      chain_condition   -> ingredients where material.type == 'intermediate'
    Confirmed correct by Jaiswal (2026-07-17): a return physically hands back
    the earring and the intermediate (e.g. the chain) -- never the raw
    material that went into making either of them, which is exactly what
    this mapping already does (raw_material is never a credit target here).
    The earlier-flagged "2 intermediate ingredients on one BOM" edge case
    isn't a real concern for this catalog -- a final product's BOM has at
    most one base_earring and one intermediate ingredient by construction
    (earring + chain = the sellable variation), so type-matching is
    unambiguous in practice, not just in the common case.

    Returns (summary_dict, updated_return_credits_rows).
    """
    from datetime import timezone as _tz
    from sheets_connector import fetch_return_receipts
    from firestore_connector import load_materials, apply_stock_movements

    summary = {'orders_credited': 0, 'movements': 0, 'errors': []}

    try:
        receipts = fetch_return_receipts()
    except Exception as e:
        summary['errors'].append(f"Return credit: fetch_return_receipts failed — {e}")
        return summary, existing_return_credits
    if not receipts:
        return summary, existing_return_credits

    try:
        materials = load_materials()
    except Exception as e:
        summary['errors'].append(f"Return credit: could not load materials — {e}")
        return summary, existing_return_credits

    _COMPONENT_TYPE = {
        'earring_condition': 'base_earring',
        'box_condition':     'packaging',
        'chain_condition':   'intermediate',
    }

    # order_id -> (platform, raw_sku) from the full persisted histories.
    order_sku = {}
    for r in fk_order_sku_index_rows:
        if r.get('order_id') and r.get('sku'):
            order_sku[r['order_id']] = ('flipkart', r['sku'])
    for r in me_order_sku_index_rows:
        if r.get('order_id') and r.get('sku_name'):
            order_sku[r['order_id']] = ('meesho', r['sku_name'])
    for r in az_orders_daily_rows:
        if r.get('order_id') and r.get('sku'):
            order_sku[r['order_id']] = ('amazon', r['sku'])

    # order_id -> AWB, for orders that also had a return filed (most orders
    # won't have an entry here, which is expected -- only returned orders
    # can possibly need a receipt at all).
    order_awb = {}
    for r in fk_order_awb_index_rows:
        if r.get('order_id') and r.get('awb'):
            order_awb[r['order_id']] = r['awb']
    for r in me_order_awb_index_rows:
        if r.get('order_id') and r.get('awb'):
            order_awb[r['order_id']] = r['awb']
    for r in az_returns_daily_rows:
        if r.get('order_id') and r.get('tracking_id'):
            order_awb[r['order_id']] = r['tracking_id']

    credits_by_order = {r['order_id']: dict(r) for r in existing_return_credits}
    movements = []

    for order_id, plat_sku in order_sku.items():
        receipt = receipts.get(order_id) or receipts.get(order_awb.get(order_id, '')) or {}
        if not receipt:
            continue

        already = credits_by_order.get(order_id, {})
        pending_components = [c for c in _COMPONENT_TYPE
                               if receipt.get(c) == 'Intact' and not already.get(c.replace('_condition', '_credited'))]
        if not pending_components:
            continue

        platform, raw_sku = plat_sku
        norm = str(raw_sku).strip().lower()
        pm_id = sku_overrides.get((platform, norm)) or pm_sku_index.get((platform, norm))
        if not pm_id:
            continue
        bom = final_boms.get(pm_id)
        if not bom or not bom.get('components'):
            continue

        credited_any = False
        for comp_condition in pending_components:
            want_type = _COMPONENT_TYPE[comp_condition]
            matching = [c for c in bom['components']
                        if (materials.get(c['material_id']) or {}).get('type') == want_type]
            if not matching:
                continue
            for c in matching:
                qty_back = float(c.get('qty_per_unit', 0) or 0)
                if qty_back <= 0:
                    continue
                movements.append({
                    'material_id': c['material_id'], 'direction': 'in', 'qty': qty_back,
                    'unit_cost': (materials.get(c['material_id']) or {}).get('current_avg_cost', 0),
                    'source_type': 'return_credit', 'source_id': order_id,
                    'date': date.today().isoformat(),
                    'notes': f"Return credit: {comp_condition} intact, {platform} order {order_id}",
                })
            credited_any = True
            already[comp_condition.replace('_condition', '_credited')] = True

        if credited_any:
            already['order_id'] = order_id
            already['credited_at'] = datetime.now(_tz.utc).isoformat()
            credits_by_order[order_id] = already
            summary['orders_credited'] += 1

    if movements:
        try:
            apply_stock_movements(movements)
            summary['movements'] = len(movements)
        except Exception as e:
            summary['errors'].append(f"Return credit: apply_stock_movements failed — {e}")

    print(f"  Return credit: {summary['orders_credited']} order(s) credited, {summary['movements']} material movement(s)")
    return summary, list(credits_by_order.values())


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    # ── Pipeline log ─────────────────────────────────────────────────────────
    _log_entries = []

    def log(status, filename, detail=''):
        """Append a log entry and print to stdout."""
        entry = f"[{status}] {filename}" + (f" — {detail}" if detail else '')
        _log_entries.append(entry)

    def flush_log():
        with open(LOG_PATH, 'a', encoding='utf-8') as lf:
            lf.write(f"\n{'='*60}\n")
            lf.write(f"  Run: {TODAY}  ({'DRY RUN' if args.dry_run else 'LIVE'})\n")
            lf.write(f"{'='*60}\n")
            for e in _log_entries:
                lf.write(e + '\n')

    print(f"\n{'='*60}")
    print(f"  Rumee Dashboard Pipeline -- {TODAY}")
    if args.dry_run:
        print("  [DRY RUN] -- DB will NOT be saved")
    print(f"{'='*60}")

    # ── --seed-users: separate path, exits early ──────────────────────────────
    if args.seed_users:
        from firestore_connector import write_user
        print("\n  [--seed-users] Seeding owner record for the whole-dashboard login...")
        write_user('rumeein@gmail.com', 'owner')
        print("  Done. Safe to publish the role-based firestore.rules now (see file-level "
              "comment in firestore.rules for the remaining pre-publish checklist).")
        return

    # ── --generate-alltime: separate path, exits early ───────────────────────
    if args.generate_alltime:
        db = load_db(DB_SUMMARY_PATH) or {}
        _run_generate_alltime(db, args)
        return

    # ── Load existing DB ──────────────────────────────────────────────────────
    db = load_db(DB_SUMMARY_PATH)
    if not db:
        print("\n  No existing DB -- creating fresh database.")
        db = {
            'config': [], 'fk_monthly': [], 'me_monthly': [],
            'fk_skus': [], 'me_skus': [], 'me_return_reasons': [], 'fk_return_reasons': [],
            'fk_pairs': [], 'az_monthly': [], 'fk_keywords': [],
            'me_claims': [], 'fk_claims': [],
        }

    # ── One-time correction: me_monthly 2026-06/2026-07 gmv/orders/returns ────
    # A now-fixed bug (me_orders_monthly.update() overwriting instead of
    # accumulating) corrupted these two persisted months' order-derived fields.
    # Settlement/ad_spend are untouched (never corrupted, only under-counted —
    # safe to let normal += accumulation fill the gap). Self-disabling via a
    # config flag so this runs exactly once.
    if not get_config(db, 'me_monthly_202606_07_corrected', default=''):
        for _mk in ('2026-06', '2026-07'):
            for _r in db.get('me_monthly', []):
                if _r.get('month') == _mk:
                    _r['gmv'], _r['orders'], _r['returns'] = 0, 0, 0
                    print(f"  [one-time correction] zeroed me_monthly {_mk} gmv/orders/returns")
        set_config(db, 'me_monthly_202606_07_corrected', TODAY)

    # ── One-time correction: me_orders_last_date backfill ─────────────────────
    # The now-fixed watermark bug didn't just freeze ME_ORDERS (like the payments
    # bugs) — it kept FALSELY ADVANCING me_orders_last_date past 13 real,
    # never-counted order-days (2026-06-19 -> 07-02), confirmed via a live run:
    # fk_payments_last_date and me_payments_last_date both self-healed forward
    # once their column fix landed, but me_orders_last_date stayed frozen at
    # 2026-07-02 (the watermark's already-corrupted peak) because the code fix
    # only stops FUTURE false-advances — it can't undo one already applied.
    # Reset back to 2026-06-18 (the last confirmed-good date) so the next run
    # recovers the 13 lost days. Self-disabling via a config flag.
    if not get_config(db, 'me_orders_watermark_20260619_backfilled', default=''):
        if get_config(db, 'me_orders_last_date', default='') > '2026-06-18':
            set_config(db, 'me_orders_last_date', '2026-06-18')
            print("  [one-time correction] reset me_orders_last_date to 2026-06-18 to recover 13 lost order-days")
        set_config(db, 'me_orders_watermark_20260619_backfilled', TODAY)

    # ── One-time correction: Meesho orders watermark-race backfill (item #66,
    #    2026-07-18) ───────────────────────────────────────────────────────────
    # A DIFFERENT, separately-caused bug -- the per-run watermark race
    # condition just fixed above in this same commit (process_meesho_orders
    # was being compared against a mid-loop-advancing watermark, so an
    # out-of-order file within one run got silently discarded, its file
    # still marked processed, never retried). Confirmed real (not a business
    # slowdown): Jaiswal confirmed real Meesho orders were arriving daily
    # the whole window. me_monthly's 2026-06/2026-07 gmv/orders/returns were
    # already zeroed by the DIFFERENT, EARLIER one-time correction above
    # (a since-fixed me_orders_monthly.update()-not-accumulate bug) and were
    # expected to self-heal via normal += accumulation on later runs -- that
    # never happened, because this watermark race kept blocking every
    # subsequent run's reprocessing attempt the whole time. Since
    # me_monthly's June/July fields are already sitting at a clean zero,
    # this correction does NOT need to zero anything itself -- it only needs
    # to make the affected files eligible for reprocessing again, and normal
    # += accumulation on the next run will correctly refill them from real
    # data now that the race condition is fixed.
    #
    # Scope: ME_ORDERS files dated 2026-06-01 through 2026-07-16 (the
    # confirmed-affected window -- me_orders_last_date was stuck at
    # 2026-07-16, and May's monthly total already looks correct/healthy, so
    # nothing before June 1 needs touching). Clears each affected file's
    # processed_file marker (so the Drive-fetch step treats it as new again)
    # and resets me_orders_last_date back to 2026-05-31 (last confirmed-good
    # date) so process_meesho_orders's own row-level filter doesn't
    # re-discard the recovered rows.
    #
    # NOT covered by this correction, left as a known, explicitly-flagged
    # gap rather than guessed at: me_skus is a running ALL-TIME accumulator,
    # not month-keyed like me_monthly, so its June/July contribution can't
    # be cleanly isolated and zeroed the same way -- reprocessing will add
    # the recovered rows on top of whatever me_skus already holds, which may
    # already be correct or may carry its own drift from the same root
    # cause. Also not covered: me_returns_last_date/me_payments_last_date
    # have the identical latent race-condition class of bug (fixed going
    # forward by this same commit) but show no CONFIRMED symptom the way
    # orders did -- not backfilled here without evidence they were actually
    # affected.
    #
    # Self-disabling via a config flag so this runs exactly once.
    if not get_config(db, 'me_orders_watermark_race_20260601_0716_backfilled', default=''):
        import re as _re_backfill
        _backfill_cleared = 0
        for _cfg_row in list(db.get('config', [])):
            _cfg_key = _cfg_row.get('key', '')
            if not _cfg_key.startswith('processed_file:me_orders_'):
                continue
            _date_match = _re_backfill.search(r'(\d{4}-\d{2}-\d{2})', _cfg_key)
            if _date_match and '2026-06-01' <= _date_match.group(1) <= '2026-07-16':
                db['config'].remove(_cfg_row)
                _backfill_cleared += 1
        if get_config(db, 'me_orders_last_date', default='') > '2026-05-31':
            set_config(db, 'me_orders_last_date', '2026-05-31')
        set_config(db, 'me_orders_watermark_race_20260601_0716_backfilled', TODAY)
        print(f"  [one-time correction] cleared {_backfill_cleared} processed_file marker(s) and reset "
              f"me_orders_last_date to 2026-05-31 to recover the Jun1-Jul16 watermark-race window")

    # ── One-time correction: ME/AZ returns re-bucketed to closure date
    #    (Delivered Date / Return Delivery Date, instead of the date a
    #    return was initiated) -- active.md item #70, 2026-07-20 ────────────
    # A full historical re-bucket, not a bounded bug-window fix -- every
    # ME_RETURNS file needs reprocessing under the new date column, and
    # Amazon's watermark needs rewinding so the SP-API report re-pulls under
    # the new column too. Zeroed first: the two Meesho accumulator targets
    # actually fed by process_meesho_returns's output, confirmed by tracing
    # merge_me_skus/build_return_reasons call sites (process.py:7153,7179) --
    # me_skus' return-derived fields (+= accumulation) and me_return_reasons
    # (also += accumulation). me_monthly's own 'returns' field is fed by
    # process_meesho_orders' RTO_COMPLETE status count -- a separate path
    # untouched by this change, confirmed by tracing where m['returns'] is
    # incremented, not guessed. az_returns_daily is a dict keyed by order_id
    # (idempotent overwrite on reprocess, process.py:7430-7433), so it needs
    # no zeroing, only its watermark rewound. FK is untouched -- it already
    # buckets by Completed Date. Self-disabling via a config flag.
    if not get_config(db, 'me_az_returns_closure_rebucket_20260720_backfilled', default=''):
        _returns_sentinel = '1970-01-01'
        _re_returns_cleared = 0
        for _cfg_row in list(db.get('config', [])):
            if _cfg_row.get('key', '').startswith('processed_file:me_returns_'):
                db['config'].remove(_cfg_row)
                _re_returns_cleared += 1
        set_config(db, 'me_returns_last_date', _returns_sentinel)
        set_config(db, 'az_returns_last_date', _returns_sentinel)
        for _r in db.get('me_skus', []):
            _r['cust_returns'] = 0
            _r['incomplete'] = 0
            _r['wrong_product'] = 0
            _r['quality'] = 0
        db['me_return_reasons'] = []
        set_config(db, 'me_az_returns_closure_rebucket_20260720_backfilled', TODAY)
        print(f"  [one-time correction] cleared {_re_returns_cleared} me_returns processed_file marker(s), "
              f"reset me_returns_last_date/az_returns_last_date to {_returns_sentinel}, and zeroed "
              f"me_skus return fields + me_return_reasons to re-bucket all history under closure date")

    # ── Optional one-off Amazon Orders/Returns backfill ───────────────────────
    if getattr(args, 'az_backfill_start', None):
        from datetime import timedelta as _az_td
        _start = datetime.strptime(args.az_backfill_start, '%Y-%m-%d').date()
        _watermark = (_start - _az_td(days=1)).isoformat()
        set_config(db, 'az_orders_last_date', _watermark)
        set_config(db, 'az_returns_last_date', _watermark)
        print(f"  [--az-backfill-start] az_orders_last_date/az_returns_last_date set to {_watermark} "
              f"so the next report request starts exactly from {args.az_backfill_start}")

    # ── Optional one-off Orders Ledger sheet ID set ───────────────────────────
    if getattr(args, 'set_ledger_sheet_id', None):
        set_config(db, 'ledger_sheet_id', args.set_ledger_sheet_id)
        print(f"  [--set-ledger-sheet-id] ledger_sheet_id set to {args.set_ledger_sheet_id}")

    # ── Optional reset ────────────────────────────────────────────────────────
    if args.reset_db:
        print("\n  [--reset-db] Full clean slate...")
        # 1. Clear all data tables
        for t in ['fk_monthly', 'me_monthly', 'fk_skus', 'me_skus',
                  'me_return_reasons', 'fk_return_reasons', 'fk_pairs', 'az_monthly', 'fk_keywords',
                  'me_claims', 'fk_claims']:
            db[t] = []
        # 2. Reset all *_last_date cutoffs → 1970-01-01 (process ALL historical rows)
        last_date_keys = [
            'me_orders_last_date', 'me_returns_last_date', 'me_payments_last_date',
            'me_ads_last_date', 'me_ads_summary_last_date', 'me_ads_catalog_last_date',
            'fk_payments_last_date', 'fk_ads_last_date',
            'fk_views_last_date', 'fk_keywords_last_date',
            'me_claims_last_date', 'fk_claims_last_date',
            'fk_listings_last_date', 'me_catalog_last_date', 'fk_returns_last_date',
        ]
        for k in last_date_keys:
            set_config(db, k, '1970-01-01')
        # 3. Remove all processed_file:* and processed_modified:* keys (so Drive re-downloads everything)
        db['config'] = [r for r in db.get('config', [])
                        if not str(r.get('key', '')).startswith('processed_file:')
                        and not str(r.get('key', '')).startswith('processed_modified:')]
        # 4. Also wipe daily + keywords + ads CSVs so they rebuild from scratch
        for p in [DB_DAILY_PATH, DB_KEYWORDS_PATH, DB_ME_ADS_PATH]:
            if p.exists():
                p.unlink()
        print(f"  Cleared data tables, reset {len(last_date_keys)} date cutoffs, "
              f"cleared Drive file cache.")

    # ── Find files ────────────────────────────────────────────────────────────
    source_files = []
    drive_paths    = set()   # Paths that came from Drive (need marking + temp cleanup)
    drive_modtimes = {}      # {path: Drive modifiedTime} for recheck-by-modtime files

    if args.source == 'drive':
        try:
            from drive_connector import fetch_new_files, test_auth
            print(f"\n  Verifying Drive credentials...")
            test_auth()   # raises immediately if credentials are missing or invalid
            print(f"  Drive: authenticated successfully")
            print(f"\n  Scanning Google Drive folders...")
            drive_results = fetch_new_files(db)
            if drive_results:
                source_files   = drive_results
                drive_paths    = {fp for fp, _, _mt in drive_results}
                drive_modtimes = {fp: mt for fp, _, mt in drive_results if mt}
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
        # Do NOT exit early -- Amazon's SP-API acquisition, the Firestore
        # push, pipeline_run_log.json, and the Discord notification are all
        # independent of Drive and must still run (dashboard memory active.md
        # #57, 2026-07-14 review). This early return predates all of them
        # (git history: introduced 2026-05-23, before Drive/Amazon/health-
        # log/Discord existed) and was never revisited -- confirmed via 4
        # recent scheduled runs that Drive normally has files, so this was a
        # latent gap, not something actively relied on. Every loop below
        # iterates an empty source_files/typed safely (no-op) when this
        # branch is taken.
        print("\n  No new Drive files this run -- continuing (Amazon/health-log/Discord still run).")

    # Sort files so older monthly files (01_2026, 02_2026...) come before newer ones.
    # This ensures date-cutoff deduplication doesn't accidentally skip historical data
    # when multiple monthly files are processed in a single run.
    source_files.sort(key=lambda x: x[0].name)

    # global, not local -- see module-level declaration near LOG_PATH for why
    # (lets standalone parser functions append directly on failure).
    global _run_errors, _run_warnings
    _run_errors   = []   # [{file, type, reason, impact}] — hard failures
    _run_warnings = []   # [{file, type, reason, impact}] — zero-row / soft issues

    # ── Amazon report requests — fired FIRST, checked LATER (2026-07-15, ─────
    # Jaiswal) so Flipkart/Meesho processing below (several minutes) doubles
    # as Amazon's report-preparation wait, instead of that wait being wasted
    # after Amazon is already checked. Only fires a NEW request if none is
    # currently pending — never polls/downloads here (see _az_poll_report,
    # called after FK/ME processing, same as before).
    try:
        _az_orders_req = _az_request_report(db, 'orders')
    except Exception as _e:
        _az_orders_req = {'status': 'failed', 'warnings': [], 'errors': [f"AZ orders: request crashed — {_e}"]}
    for _w in _az_orders_req['warnings']:
        _run_warnings.append({'file': 'amazon_orders_api', 'type': 'AMAZON', 'reason': _w})
    for _e in _az_orders_req['errors']:
        _run_errors.append({'file': 'amazon_orders_api', 'type': 'AMAZON', 'reason': _e})

    try:
        _az_returns_req = _az_request_report(db, 'returns')
    except Exception as _e:
        _az_returns_req = {'status': 'failed', 'warnings': [], 'errors': [f"AZ returns: request crashed — {_e}"]}
    for _w in _az_returns_req['warnings']:
        _run_warnings.append({'file': 'amazon_returns_api', 'type': 'AMAZON', 'reason': _w})
    for _e in _az_returns_req['errors']:
        _run_errors.append({'file': 'amazon_returns_api', 'type': 'AMAZON', 'reason': _e})

    try:
        _az_sqp_req = _az_request_sqp(db)
    except Exception as _e:
        _az_sqp_req = {'status': 'failed', 'warnings': [], 'errors': [f"AZ SQP: request crashed — {_e}"]}
    for _w in _az_sqp_req['warnings']:
        _run_warnings.append({'file': 'amazon_sqp_api', 'type': 'AMAZON', 'reason': _w})
    for _e in _az_sqp_req['errors']:
        _run_errors.append({'file': 'amazon_sqp_api', 'type': 'AMAZON', 'reason': _e})

    try:
        _az_catalog_req = _az_request_catalog(db)
    except Exception as _e:
        _az_catalog_req = {'status': 'failed', 'warnings': [], 'errors': [f"AZ catalog: request crashed — {_e}"]}
    for _w in _az_catalog_req['warnings']:
        _run_warnings.append({'file': 'amazon_catalog_api', 'type': 'AMAZON', 'reason': _w})
    for _e in _az_catalog_req['errors']:
        _run_errors.append({'file': 'amazon_catalog_api', 'type': 'AMAZON', 'reason': _e})

    # ── Detect file types ─────────────────────────────────────────────────────
    print(f"\n  Found {len(source_files)} file(s):")
    typed = {}
    for fp, ft_hint, _ in source_files:
        ft = detect_file_type(fp)
        if ft == 'UNKNOWN' and ft_hint:
            ft = ft_hint   # Trust Drive folder hint when sniff fails
        typed[fp] = ft
        print(f"    {fp.name:50s} = {ft}")

    # ── Last-processed dates ──────────────────────────────────────────────────
    me_orders_last    = get_config(db, 'me_orders_last_date')
    me_returns_last   = get_config(db, 'me_returns_last_date')
    me_payments_last  = get_config(db, 'me_payments_last_date')
    me_ads_last       = get_config(db, 'me_ads_last_date')          # monthly ad-spend total (payments sheet / standalone ME_ADS)
    me_ads_summary_last = get_config(db, 'me_ads_summary_last_date')  # ME_ADS_SUMMARY → me_ads_daily (own watermark — was colliding with catalog)
    me_ads_catalog_last = get_config(db, 'me_ads_catalog_last_date')  # ME_ADS_CATALOG → me_ads_catalog (own watermark)
    fk_payments_last  = get_config(db, 'fk_payments_last_date')
    fk_ads_last       = get_config(db, 'fk_ads_last_date')
    fk_views_last     = get_config(db, 'fk_views_last_date')
    fk_keywords_last  = get_config(db, 'fk_keywords_last_date')
    me_claims_last    = get_config(db, 'me_claims_last_date')
    fk_claims_last    = get_config(db, 'fk_claims_last_date')
    fk_orders_last    = get_config(db, 'fk_orders_last_date') or '2026-01-01'
    fk_returns_last   = get_config(db, 'fk_returns_last_date') or '2026-01-01'

    # Frozen copies of every watermark above, from BEFORE this run's file
    # loop starts (active.md item #66, 2026-07-18 -- real, confirmed data
    # loss bug). Every process_X(fp, X_last) call below MUST pass the
    # _start version, never the live X_last variable, and X_last_date must
    # never be written to Firestore until AFTER the whole loop finishes.
    # Why: the loop previously wrote set_config(...) and advanced the live
    # variable immediately after EACH file, so if a run downloads several
    # files of the same type out of chronological order (confirmed real for
    # Meesho, which switched to one-file-per-day exports ~2026-05-15 -- lots
    # of small files piling up between runs, easy to land out of order), a
    # later-in-loop file dated EARLIER than one already processed this run
    # would compare its rows against an already-advanced watermark, get
    # treated as "already processed," and get silently discarded forever
    # (the file itself still gets marked processed, so it's never retried).
    # This is the exact failure mode a 2026-07 comment below already
    # documents a ONE-TIME manual correction for ("13 lost order-days") --
    # that fixed one occurrence, not the underlying bug, which kept
    # recurring (confirmed live: June+July 2026 me_monthly show ~0 real
    # orders while settlement/ad spend continued normally, and Jaiswal
    # confirmed real Meesho orders were arriving daily the whole time).
    # Freezing the comparison watermark for the whole run, then committing
    # the true max-seen value once at the end, makes file processing order
    # within a single run irrelevant -- every file this run is compared
    # against the SAME starting point, so an out-of-order file can no
    # longer be skipped by a sibling file's own advance.
    me_orders_last_start      = me_orders_last
    me_returns_last_start     = me_returns_last
    me_payments_last_start    = me_payments_last
    me_ads_last_start         = me_ads_last
    me_ads_summary_last_start = me_ads_summary_last
    me_ads_catalog_last_start = me_ads_catalog_last
    fk_payments_last_start    = fk_payments_last
    fk_ads_last_start         = fk_ads_last
    fk_views_last_start       = fk_views_last
    fk_keywords_last_start    = fk_keywords_last
    me_claims_last_start      = me_claims_last
    fk_claims_last_start      = fk_claims_last
    fk_orders_last_start      = fk_orders_last
    fk_returns_last_start     = fk_returns_last

    processed_files = []

    # ── Accumulators ──────────────────────────────────────────────────────────
    me_orders_monthly = {}
    me_orders_skus    = {}
    me_sett_monthly   = {}
    me_ads_monthly    = {}
    me_return_skus    = {}
    me_return_reasons = {}
    fk_return_reasons = {}
    me_catalog        = {}
    fk_pay_monthly    = {}
    fk_pay_skus       = {}
    fk_ads_monthly    = {}
    fk_views_skus     = {}
    fk_keywords_data  = {}
    fk_listings_pairs = []   # fk_pairs built from Listing file — replaces existing
    pm_catalog_ids    = {}   # {label_folder: {design, variation_type, platform, listings:[...]}} — Meesho + FK + Shopsy, keyed by label folder
    pm_needs_review   = []   # unmapped listings collected across FK/Meesho catalog files this run
    # pm_overrides_load_failed gates CATALOG/FK_LISTINGS/Amazon product_master
    # processing below (search this file for the flag). A failed load must
    # NEVER be treated the same as "collection is empty" — that silently
    # floods needs_review with every SKU in the run (found 2026-07-04, caused
    # by a missing-credentials failure). On failure we skip those steps
    # entirely (files stay unprocessed and retry next run) and alert Discord.
    pm_overrides_load_failed = False
    try:
        from firestore_connector import load_pm_overrides
        pm_overrides_cache = load_pm_overrides()   # loaded once per run, not per row
    except Exception as _pmov_e:
        print(f"  pm_overrides load FAILED — CATALOG/FK_LISTINGS/Amazon product_master "
              f"processing will be SKIPPED this run (files retry next run): {_pmov_e}")
        pm_overrides_cache = {}
        pm_overrides_load_failed = True
        try:
            send_discord_pm_overrides_alert(str(_pmov_e))
        except Exception as _alert_e:
            print(f"  (Discord alert for pm_overrides failure also failed: {_alert_e})")
    me_claims_rows         = []   # ME claims ticket rows (merged by ticket_id)
    fk_claims_rows         = []   # FK claims rows (merged by claim_id / order_id)
    me_ads_summary_monthly = {}   # from ME_ADS_SUMMARY daily campaign CSVs
    me_ads_daily_rows      = []   # from ME_ADS_SUMMARY (campaign-level daily detail)
    me_ads_catalog_rows    = []   # from ME_ADS_CATALOG (catalog-level daily)
    me_ads_master_rows     = []   # from ME_ADS_MASTER (lifetime snapshot, full replace)
    me_views_rows          = []   # from ME_VIEWS file
    fk_shopsy_monthly      = {}   # {month: {shopsy_orders, shopsy_revenue}}
    fk_sku_revship         = {}   # {sku_id: reverse_shipping_total}
    fk_zone_counts         = {}   # {zone: {orders, revenue, returns}}
    fk_ads_daily_rows      = []   # from FK_ADS_DAILY
    fk_ads_sku_rows        = []   # from FK_ADS_FSN
    fk_ads_kw_rows         = []   # from FK_ADS_KW
    fk_ads_placements_rows = []   # from FK_ADS_PLACEMENTS
    fk_ads_overall_rows    = []   # from FK_ADS_OVERALL
    fk_ads_search_rows     = []   # from FK_ADS_SEARCH
    fk_ads_order_rows      = []   # from FK_ADS_ORDERS
    fk_orders_daily_rows   = []   # from FK_ORDERS (Fulfilment)
    fk_ord_order_rows      = []   # from FK_ORDERS (Fulfilment) -- per-order identity rows,
                                   # feeds fk_order_sku_index as the Orders-primary base
                                   # (active.md item #67, 2026-07-19)
    fk_pay_order_rows      = []   # individual order rows for Orders Ledger (from FK_PAYMENTS)
    me_order_rows          = []   # individual order rows for Orders Ledger (from ME_ORDERS)
    # Persisted order-status registry updates (active.md item #66, 2026-07-18)
    # -- {order_id: 'delivered'|'rto'|'return'} from this run's Payments
    # files, always overrides whatever the Orders file guessed. Last-write-
    # wins across multiple files processed in one run (rare in practice).
    fk_order_statuses_new  = {}
    me_order_statuses_new  = {}
    me_order_settlements_new = {}   # {suborder_id: real settlement} from ME_PAYMENTS
                                     # (active.md item #67, 2026-07-19)
    me_return_reason_index = {}   # {suborder_id: return_reason_str} from ME_RETURNS
    me_suborder_awb_index  = {}   # {suborder_id: awb_number} from ME_RETURNS -- resolves
                                   # Return Receipts scans that only captured the AWB
    fk_order_awb_index     = {}   # {order_id: tracking_id} from FK_RETURNS -- same purpose, FK side
    me_suborder_sku_index  = {}   # {suborder_id: sku_id} from ME_RETURNS -- the return's OWN
                                   # reported SKU, preferred over the order-side guess (item #72)
    fk_return_sku_index    = {}   # {order_id: sku} from FK_RETURNS -- same purpose, FK side
    fk_orders_sku_rows     = []   # from FK_ORDERS (Fulfilment) per-SKU
    fk_returns_daily_rows  = []   # from FK_RETURNS (Fulfilment) per-date, by Completed Date
    fk_returns_sku_rows    = []   # from FK_RETURNS (Fulfilment) per-SKU

    # Path collectors for daily / keywords builders (parallel to existing flow)
    me_orders_paths   = []   # raw ME Orders files for build_me_daily
    me_returns_paths  = []   # raw ME Returns files for build_me_daily
    fk_views_paths    = []   # raw FK Views files for build_fk_daily
    fk_keywords_paths = []   # raw FK Keywords files for build_fk_keywords

    # ── Process each file ─────────────────────────────────────────────────────
    import traceback as _tb
    # _run_errors/_run_warnings initialized earlier, before the Amazon
    # report-request phase, so those results land in the same run's log.

    def _log_fail(fp, ft, reason):
        msg = f"[FAIL] {fp.name} ({ft}) — {reason}"
        _log_entries.append(msg)
        _run_errors.append({'file': fp.name, 'type': ft, 'reason': reason})
        print(f"  ERROR: {reason}")

    def _log_warn(fp, ft, reason):
        msg = f"[WARN] {fp.name} ({ft}) — {reason}"
        _log_entries.append(msg)
        _run_warnings.append({'file': fp.name, 'type': ft, 'reason': reason})
        print(f"  WARN: {reason}")

    for fp, ft in typed.items():
        print(f"\n  Processing: {fp.name} ({ft})")
        _before = len(processed_files)

        if ft == 'ME_ORDERS':
            me_orders_paths.append(fp)          # collect for build_me_daily
            try:
                m, s, new_last, ord_rows = process_meesho_orders(fp, me_orders_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for mk, nd in m.items():
                if mk in me_orders_monthly:
                    for k in nd:
                        me_orders_monthly[mk][k] = me_orders_monthly[mk].get(k, 0) + nd[k]
                else:
                    me_orders_monthly[mk] = dict(nd)
            for sid, nd in s.items():
                if sid in me_orders_skus:
                    me_orders_skus[sid]['delivered'] += nd['delivered']
                    me_orders_skus[sid]['rto']       += nd['rto']
                    me_orders_skus[sid]['gmv']       += nd['gmv']
                    # orders/cancelled/total_orders were silently NOT
                    # accumulated here before (pre-existing gap, unrelated
                    # to today's fix) -- adding orders explicitly since
                    # merge_me_skus now depends on it for total_orders/
                    # avg_price (2026-07-18, "nothing should be tied to
                    # being Delivered for calculating orders and GMV").
                    me_orders_skus[sid]['orders']    = me_orders_skus[sid].get('orders', 0) + nd.get('orders', 0)
                    me_orders_skus[sid]['cancelled']  = me_orders_skus[sid].get('cancelled', 0) + nd.get('cancelled', 0)
                else:
                    me_orders_skus[sid] = nd
            me_order_rows.extend(ord_rows)
            if new_last > me_orders_last:
                me_orders_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)
            if not m and not s:
                _log_warn(fp, ft, 'parsed but produced 0 rows — file may be empty or wrong format')

        elif ft == 'ME_RETURNS':
            me_returns_paths.append(fp)         # collect for build_me_daily
            try:
                sr, reasons, new_last, subord_idx, subord_awb_idx, subord_sku_idx = process_meesho_returns(fp, me_returns_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for sid, nd in sr.items():
                if sid in me_return_skus:
                    for k in nd:
                        me_return_skus[sid][k] = me_return_skus[sid].get(k, 0) + nd[k]
                else:
                    me_return_skus[sid] = dict(nd)
            for r, c in reasons.items():
                me_return_reasons[r] = me_return_reasons.get(r, 0) + c
            me_return_reason_index.update(subord_idx)
            me_suborder_awb_index.update(subord_awb_idx)
            me_suborder_sku_index.update(subord_sku_idx)
            if new_last > me_returns_last:
                me_returns_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'ME_PAYMENTS':
            # Returns 6-tuple: (monthly_sett, monthly_ads, pay_new_last, ads_new_last, order_statuses, order_settlements)
            try:
                m, m_ads, pay_new_last, ads_new_last, order_statuses, order_settlements = process_meesho_payments(
                    fp, me_payments_last_start, me_ads_last_start
                )
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for mk, sett in m.items():
                me_sett_monthly[mk] = me_sett_monthly.get(mk, 0) + sett
            for mk, ads in m_ads.items():
                me_ads_monthly[mk] = me_ads_monthly.get(mk, 0) + ads
            me_order_statuses_new.update(order_statuses)
            me_order_settlements_new.update(order_settlements)
            if pay_new_last > me_payments_last:
                me_payments_last = pay_new_last  # committed to Firestore once, after the loop
            if m_ads and ads_new_last > me_ads_last:
                me_ads_last = ads_new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)
            if not m:
                _log_warn(fp, ft, 'parsed but produced 0 monthly rows')

        elif ft == 'ME_ADS':
            try:
                m, new_last = process_meesho_ads(fp, me_ads_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for mk, ads in m.items():
                me_ads_monthly[mk] = me_ads_monthly.get(mk, 0) + ads
            if new_last > me_ads_last:
                me_ads_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_PAYMENTS':
            # Returns 10-tuple: (monthly, skus, monthly_ads, monthly_shopsy, sku_revship, zone_counts, pay_new_last, ads_new_last, order_rows, order_statuses)
            try:
                m, s, m_ads, m_shopsy, s_revship, z_counts, pay_new_last, ads_new_last, pay_order_rows, order_statuses = process_fk_payments(
                    fp, fk_payments_last_start, fk_ads_last_start
                )
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
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
            for mk, sh in m_shopsy.items():
                if mk in fk_shopsy_monthly:
                    fk_shopsy_monthly[mk]['shopsy_orders']  += sh['shopsy_orders']
                    fk_shopsy_monthly[mk]['shopsy_revenue']  = round(
                        fk_shopsy_monthly[mk]['shopsy_revenue'] + sh['shopsy_revenue'], 2)
                else:
                    fk_shopsy_monthly[mk] = dict(sh)
            for sid, cost in s_revship.items():
                fk_sku_revship[sid] = round(fk_sku_revship.get(sid, 0) + cost, 2)
            for zone, nd in z_counts.items():
                if zone in fk_zone_counts:
                    fk_zone_counts[zone]['orders']  += nd['orders']
                    fk_zone_counts[zone]['revenue']  = round(fk_zone_counts[zone]['revenue'] + nd['revenue'], 2)
                    fk_zone_counts[zone]['returns'] += nd['returns']
                else:
                    fk_zone_counts[zone] = dict(nd)
            fk_pay_order_rows.extend(pay_order_rows)
            fk_order_statuses_new.update(order_statuses)
            if pay_new_last > fk_payments_last:
                fk_payments_last = pay_new_last  # committed to Firestore once, after the loop
            if m_ads and ads_new_last > fk_ads_last:
                fk_ads_last = ads_new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_ADS':
            try:
                m, new_last = process_fk_ads(fp, fk_ads_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for mk, ads in m.items():
                fk_ads_monthly[mk] = fk_ads_monthly.get(mk, 0) + ads
            if new_last > fk_ads_last:
                fk_ads_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_ADS_CAMPAIGN':
            try:
                camp_skus, _ = process_fk_ads_campaign(fp)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            # Merge campaign ad-performance data into fk_views_skus accumulator
            # (same merge path as FK_VIEWS — updates ad_views, ctr, ad_revenue, conversions)
            for sid, nd in camp_skus.items():
                if sid in fk_views_skus:
                    for k in ('ad_views', 'clicks', 'conversions'):
                        fk_views_skus[sid][k] = fk_views_skus[sid].get(k, 0) + nd.get(k, 0)
                    fk_views_skus[sid]['ad_revenue'] = round(
                        _flt(fk_views_skus[sid].get('ad_revenue', 0))
                        + _flt(nd.get('ad_revenue', 0)), 2
                    )
                    fk_views_skus[sid]['ad_spend'] = round(
                        _flt(fk_views_skus[sid].get('ad_spend', 0))
                        + _flt(nd.get('ad_spend', 0)), 2
                    )
                else:
                    fk_views_skus[sid] = dict(nd)
            processed_files.append(fp)

        elif ft == 'FK_VIEWS':
            fk_views_paths.append(fp)           # collect for build_fk_daily
            try:
                s, new_last = process_fk_views(fp, fk_views_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for sid, nd in s.items():
                if sid in fk_views_skus:
                    for k in ('ad_views', 'clicks', 'sales', 'ad_revenue'):
                        fk_views_skus[sid][k] = fk_views_skus[sid].get(k, 0) + nd.get(k, 0)
                else:
                    fk_views_skus[sid] = dict(nd)
            if new_last > fk_views_last:
                fk_views_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_KEYWORDS':
            fk_keywords_paths.append(fp)        # collect for build_fk_keywords
            try:
                kw, new_last = process_fk_keywords(fp, fk_keywords_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for kw_name, nd in kw.items():
                if kw_name in fk_keywords_data:
                    for k in ('views', 'clicks', 'orders'):
                        fk_keywords_data[kw_name][k] = (
                            fk_keywords_data[kw_name].get(k, 0) + nd.get(k, 0)
                        )
                    fk_keywords_data[kw_name]['revenue'] = round(
                        _flt(fk_keywords_data[kw_name].get('revenue', 0))
                        + _flt(nd.get('revenue', 0)), 2
                    )
                else:
                    fk_keywords_data[kw_name] = dict(nd)
            if new_last > fk_keywords_last:
                fk_keywords_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_LISTINGS':
            if pm_overrides_load_failed:
                print(f"  Skipping {fp.name} (FK_LISTINGS) — pm_overrides failed to load this run"); continue
            try:
                pairs, _fsn_map, nr, fk_entries = process_fk_listings(fp, pm_overrides=pm_overrides_cache)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            if pairs:
                fk_listings_pairs = pairs  # full replace — listing file is master data
                set_config(db, 'fk_listings_last_date', TODAY)
            # Merge FK/Shopsy product_master listings into the unified label-keyed
            # dict. Dedup key MUST match the writer's _mkey (product_id-or-catalog_id)
            # — Meesho is per-product_id, so a plain catalog_id key would silently
            # drop distinct products that share one Meesho catalog. FSN travels
            # inside each listing entry (no bare slug-doc write → no orphan docs).
            _merge_pm_entries(pm_catalog_ids, fk_entries)
            pm_needs_review.extend(nr)
            processed_files.append(fp)

        elif ft == 'CATALOG':
            if pm_overrides_load_failed:
                print(f"  Skipping {fp.name} (CATALOG) — pm_overrides failed to load this run"); continue
            try:
                me_catalog, cat_ids, nr = process_catalog(fp, pm_overrides=pm_overrides_cache)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            pm_needs_review.extend(nr)
            if me_catalog:
                set_config(db, 'me_catalog_last_date', TODAY)
            _merge_pm_entries(pm_catalog_ids, cat_ids)
            processed_files.append(fp)

        elif ft == 'ME_CLAIMS':
            try:
                new_rows, new_last = process_meesho_claims(fp, me_claims_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            me_claims_rows.extend(new_rows)
            if new_last > me_claims_last:
                me_claims_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_CLAIMS':
            try:
                new_rows, new_last = process_flipkart_claims(fp, fk_claims_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            fk_claims_rows.extend(new_rows)
            if new_last > fk_claims_last:
                fk_claims_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'ME_ADS_SUMMARY':
            try:
                m, camp_rows, new_last = process_me_ads_summary(fp, me_ads_summary_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            for mk, ads in m.items():
                me_ads_summary_monthly[mk] = round(
                    me_ads_summary_monthly.get(mk, 0) + ads, 2)
            me_ads_daily_rows.extend(camp_rows)
            if new_last > me_ads_summary_last:
                me_ads_summary_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'ME_VIEWS':
            try:
                rows = process_me_views(fp)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            me_views_rows.extend(rows)
            processed_files.append(fp)

        elif ft == 'FK_ADS_DAILY':
            try:
                fk_ads_daily_rows.extend(process_fk_ads_daily(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_FSN':
            try:
                fk_ads_sku_rows.extend(process_fk_ads_fsn(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_KW':
            try:
                fk_ads_kw_rows.extend(process_fk_ads_kw(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_PLACEMENTS':
            try:
                fk_ads_placements_rows.extend(process_fk_ads_placements(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_OVERALL':
            try:
                fk_ads_overall_rows.extend(process_fk_ads_overall(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_SEARCH':
            try:
                fk_ads_search_rows.extend(process_fk_ads_search(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'FK_ADS_ORDERS':
            try:
                fk_ads_order_rows.extend(process_fk_ads_orders(fp))
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            processed_files.append(fp)

        elif ft == 'ME_ADS_CATALOG':
            try:
                cat_rows, new_last = process_me_ads_catalog(fp, me_ads_catalog_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            me_ads_catalog_rows.extend(cat_rows)
            if new_last > me_ads_catalog_last:
                me_ads_catalog_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'ME_ADS_MASTER':
            try:
                rows = process_me_ads_master(fp)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            if rows:
                me_ads_master_rows = rows  # full replace — lifetime snapshot
            processed_files.append(fp)

        elif ft == 'FK_ORDERS':
            try:
                d_rows, s_rows, o_rows, new_last = process_fk_orders(fp, fk_orders_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            fk_orders_daily_rows.extend(d_rows)
            fk_orders_sku_rows.extend(s_rows)
            fk_ord_order_rows.extend(o_rows)
            if new_last > fk_orders_last:
                fk_orders_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        elif ft == 'FK_RETURNS':
            try:
                d_rows, s_rows, reasons, new_last, order_awb_idx, order_sku_idx = process_fk_returns(fp, fk_returns_last_start)
            except Exception as _e:
                _log_fail(fp, ft, f"{type(_e).__name__}: {_e}"); _tb.print_exc(); continue
            fk_returns_daily_rows.extend(d_rows)
            fk_returns_sku_rows.extend(s_rows)
            for r, c in reasons.items():
                fk_return_reasons[r] = fk_return_reasons.get(r, 0) + c
            fk_order_awb_index.update(order_awb_idx)
            fk_return_sku_index.update(order_sku_idx)
            if new_last > fk_returns_last:
                fk_returns_last = new_last  # committed to Firestore once, after the loop
            processed_files.append(fp)

        else:
            print(f"  UNKNOWN file type -- skipping {fp.name}")
            log('SKIP', fp.name, ft)

        # Log pass/fail based on whether this file was added to processed_files
        if len(processed_files) > _before:
            log('PASS', fp.name, ft)
        elif ft not in ('UNKNOWN',):
            _log_warn(fp, ft, 'added to processed list but produced 0 new rows')

    # Commit every watermark's true max-seen value ONCE, now that the whole
    # file loop has finished (active.md item #66, 2026-07-18) -- see the
    # long comment where the _start copies were frozen, above, for why this
    # must happen here and not per-file inside the loop.
    _watermark_commits = [
        ('me_orders_last_date',        me_orders_last,        me_orders_last_start),
        ('me_returns_last_date',       me_returns_last,       me_returns_last_start),
        ('me_payments_last_date',      me_payments_last,      me_payments_last_start),
        ('me_ads_last_date',           me_ads_last,           me_ads_last_start),
        ('me_ads_summary_last_date',   me_ads_summary_last,   me_ads_summary_last_start),
        ('me_ads_catalog_last_date',   me_ads_catalog_last,   me_ads_catalog_last_start),
        ('fk_payments_last_date',      fk_payments_last,      fk_payments_last_start),
        ('fk_ads_last_date',           fk_ads_last,           fk_ads_last_start),
        ('fk_views_last_date',         fk_views_last,         fk_views_last_start),
        ('fk_keywords_last_date',      fk_keywords_last,      fk_keywords_last_start),
        ('me_claims_last_date',        me_claims_last,        me_claims_last_start),
        ('fk_claims_last_date',        fk_claims_last,        fk_claims_last_start),
        ('fk_orders_last_date',        fk_orders_last,        fk_orders_last_start),
        ('fk_returns_last_date',       fk_returns_last,       fk_returns_last_start),
    ]
    for _cfg_key, _final_val, _start_val in _watermark_commits:
        if _final_val != _start_val:
            set_config(db, _cfg_key, _final_val)

    if not processed_files:
        print("\n  No files were processed successfully.")
        # Even when nothing new landed (or the only new file(s) failed), still
        # refresh the parts of pipeline_run_log.json that don't depend on a
        # merge: run health (this run may have real errors/warnings from files
        # that failed) and the Auto-Sync manifest cross-check (reads db/config
        # state directly — see _build_manifest_cross_check, it never needed the
        # daily-table merge below). Confirmed live 2026-07-11: without this, a
        # single failing file in an otherwise-empty run silently froze the
        # whole diagnostics section (including this cross-check) on whatever
        # the previous successful run had written, with no visible indication
        # anything was stale. stream_gaps/stream_status/stream_rows are left
        # untouched here — those genuinely depend on the daily-table merge
        # below, which has nothing new to contribute this run.
        try:
            from drive_connector import fetch_download_manifest as _fdm_early
            _rows_early = _fdm_early()
        except Exception as _e_early:
            print(f"  Manifest cross-check: unavailable this run ({_e_early})")
            _rows_early = []

        _daily_early = {'me_views': set(r['date'] for r in db.get('me_views', []) if r.get('date'))}
        for _sid_early, _pfx_early in _STREAM_FILE_PREFIXES.items():
            _daily_early[_sid_early] = _dated_processed_files(db, *_pfx_early)
        _cc_early = _build_manifest_cross_check(_rows_early, _daily_early, TODAY)

        _rl_path_early = BASE_DIR / 'pipeline_run_log.json'
        try:
            _existing_early = json.loads(_rl_path_early.read_text(encoding='utf-8')) if _rl_path_early.exists() else {}
        except Exception:
            _existing_early = {}
        _existing_early['last_run'] = datetime.now().isoformat()[:19]
        _existing_early['run_status'] = 'failed' if _run_errors else ('warning' if _run_warnings else 'ok')
        _existing_early['errors'] = _run_errors
        _existing_early['warnings'] = _run_warnings
        _existing_early['manifest_cross_check'] = _cc_early
        try:
            with open(_rl_path_early, 'w', encoding='utf-8') as _f_early:
                json.dump(_existing_early, _f_early, indent=2)
            print("  pipeline_run_log.json updated (health + manifest cross-check only — no new files to merge)")
        except Exception as _e_write:
            print(f"  Warning: could not write pipeline_run_log.json — {_e_write}")
            _run_warnings.append({'file': 'pipeline_run_log.json', 'type': 'INFRA', 'reason': f"could not write early-return pipeline_run_log.json: {_e_write}",
                                   'impact': "the Data Pipeline Map may show a stale run log until a future run succeeds in writing it"})
        # Do NOT return here -- same reason as the earlier "no source files"
        # check (dashboard memory active.md #57, 2026-07-14): Amazon's SP-API
        # acquisition, the full pipeline_run_log rebuild, the Firestore push,
        # and Discord all sit further down and are independent of whether any
        # FK/ME file was processed this run. The partial write just above is
        # a safety net (2026-07-11) in case something later crashes -- it
        # gets safely superseded by the fuller run-log write below once this
        # falls through instead of returning.

    # ── Mark Drive files as processed ─────────────────────────────────────────
    # Write both keys: fetch_new_files() dedup-checks processed_file: for most
    # folders and processed_modified: only for folders in _RECHECK_BY_MODTIME
    # (ME_VIEWS, ME_ADS_MASTER). Writing both means whichever key a folder's
    # check uses, it finds a match — folders using processed_file: no longer
    # get silently re-downloaded and re-processed every run.
    for fp in processed_files:
        if fp in drive_paths:
            mt = drive_modtimes.get(fp, '')
            set_config(db, f'processed_file:{fp.name}', TODAY)
            if mt:
                set_config(db, f'processed_modified:{fp.name}', mt)

    # ── Dry run exit ──────────────────────────────────────────────────────────
    if args.dry_run:
        print(f"\n  [DRY RUN] Processed {len(processed_files)} file(s). DB not saved.")
        return

    # ── Merge into DB ─────────────────────────────────────────────────────────
    print("\n  Merging into database...")
    import traceback as _tb_merge

    if fk_pay_monthly or fk_ads_monthly or fk_shopsy_monthly:
        print("  STEP: merge fk_monthly")
        db['fk_monthly'] = merge_monthly(
            db.get('fk_monthly', []), fk_pay_monthly, 'fk', new_ads=fk_ads_monthly,
            new_shopsy=fk_shopsy_monthly,
        )

    if me_orders_monthly or me_sett_monthly or me_ads_monthly or me_ads_summary_monthly:
        print("  STEP: merge me_monthly")
        combined_me_ads = dict(me_ads_monthly)
        for mk, ads in me_ads_summary_monthly.items():
            combined_me_ads[mk] = round(combined_me_ads.get(mk, 0) + ads, 2)
        db['me_monthly'] = merge_monthly(
            db.get('me_monthly', []), me_orders_monthly, 'me',
            new_sett=me_sett_monthly, new_ads=combined_me_ads
        )

    if me_orders_skus or me_return_skus or me_catalog:
        print("  STEP: merge me_skus")
        db['me_skus'] = merge_me_skus(
            db.get('me_skus', []), me_orders_skus, me_return_skus, me_catalog
        )

    if fk_pay_skus or fk_views_skus or fk_sku_revship:
        print("  STEP: merge fk_skus")
        db['fk_skus'] = merge_fk_skus(
            db.get('fk_skus', []), fk_pay_skus, fk_views_skus,
            new_reverse_ship=fk_sku_revship,
        )

    if me_orders_paths:
        print("  STEP: build me_state_summary")
        new_state_rows = build_me_state_summary(me_orders_paths)
        if new_state_rows:
            db['me_state_summary'] = merge_me_state_summary(
                db.get('me_state_summary', []), new_state_rows
            )

    if fk_zone_counts:
        print("  STEP: merge fk_zone_summary")
        db['fk_zone_summary'] = merge_fk_zone_summary(
            db.get('fk_zone_summary', []), fk_zone_counts
        )

    if me_return_reasons:
        db['me_return_reasons'] = build_return_reasons(
            db.get('me_return_reasons', []), me_return_reasons
        )

    if fk_return_reasons:
        db['fk_return_reasons'] = build_return_reasons(
            db.get('fk_return_reasons', []), fk_return_reasons
        )

    if fk_keywords_data:
        db['fk_keywords'] = merge_fk_keywords(
            db.get('fk_keywords', []), fk_keywords_data
        )

    if fk_listings_pairs:
        print("  STEP: fk_pairs replace")
        db['fk_pairs'] = fk_listings_pairs  # replace each run — listing file is master data

    if pm_catalog_ids:
        print("  STEP: product_master label-folder write (Meesho + Flipkart + Shopsy)")
        try:
            from firestore_connector import write_product_master_ids
            write_product_master_ids(pm_catalog_ids)
        except Exception as _pm_e:
            print(f"  product_master enrich skipped: {_pm_e}")
            _run_warnings.append({'file': 'product_master_ids', 'type': 'CATALOG', 'reason': f"product_master label-folder write failed: {_pm_e}",
                                   'impact': "Meesho/Flipkart/Shopsy product_master catalog enrichment was skipped this run — Products tab catalog mapping won't reflect this run's new listings"})

    if pm_overrides_load_failed:
        print("  STEP: product_master Amazon catalog enrichment — SKIPPED (pm_overrides failed to load this run)")
    else:
        print("  STEP: product_master Amazon catalog enrichment")
        try:
            az_listings, az_nr = process_az_catalog_for_pm(pm_overrides=pm_overrides_cache)
            pm_needs_review.extend(az_nr)
            if az_listings:
                from firestore_connector import write_az_product_master
                write_az_product_master(az_listings)
        except Exception as _az_e:
            print(f"  product_master Amazon enrich skipped: {_az_e}")
            _run_warnings.append({'file': 'az_product_master', 'type': 'AMAZON', 'reason': f"product_master Amazon catalog enrichment failed: {_az_e}",
                                   'impact': "Amazon product_master catalog enrichment was skipped this run — Products tab won't reflect this run's Amazon catalog changes"})

    if pm_needs_review:
        print("  STEP: needs_review upsert (unmapped SKUs, root-cause fix — never auto-slugified)")
        try:
            from firestore_connector import write_needs_review
            write_needs_review(pm_needs_review)
        except Exception as _nr_e:
            print(f"  needs_review upsert skipped: {_nr_e}")
            _run_warnings.append({'file': 'needs_review', 'type': 'CATALOG', 'reason': f"needs_review upsert failed: {_nr_e}",
                                   'impact': "unmapped SKUs found this run weren't recorded — some listings needing manual mapping in Products tab may be missing from the Needs Review queue"})

    if me_claims_rows:
        db['me_claims'] = merge_claims(
            db.get('me_claims', []), me_claims_rows, 'ticket_id'
        )

    if fk_claims_rows:
        db['fk_claims'] = merge_claims(
            db.get('fk_claims', []), fk_claims_rows, 'order_id'
        )

    if me_views_rows:
        ex_views = {r['date']: r for r in db.get('me_views', [])}
        for r in me_views_rows:
            ex_views[r['date']] = r
        db['me_views'] = sorted(ex_views.values(), key=lambda r: r['date'])

    print("  STEP: save_db summary")
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

    ex_fk_ord_d = {r['date']: r for r in existing_daily.get('fk_orders_daily', [])}
    for r in fk_orders_daily_rows:
        ex_fk_ord_d[r['date']] = r
    fk_orders_daily_rows = sorted(ex_fk_ord_d.values(), key=lambda r: r['date'])

    # Captured before the merge below overwrites fk_orders_sku_rows with the
    # full existing+new history -- stock decrement (active.md item #64) only
    # ever wants to move stock for genuinely new-this-run order lines, same
    # "new rows this run" guarantee the Orders Ledger already relies on.
    fk_orders_sku_new = list(fk_orders_sku_rows)

    ex_fk_ord_s = {(r['date'], r['sku']): r for r in existing_daily.get('fk_orders_sku', [])}
    for r in fk_orders_sku_rows:
        ex_fk_ord_s[(r['date'], r['sku'])] = r
    fk_orders_sku_rows = sorted(ex_fk_ord_s.values(), key=lambda r: (r['date'], r['sku']))

    ex_fk_ret_d = {r['date']: r for r in existing_daily.get('fk_returns_daily', [])}
    for r in fk_returns_daily_rows:
        ex_fk_ret_d[r['date']] = r
    fk_returns_daily_rows = sorted(ex_fk_ret_d.values(), key=lambda r: r['date'])

    ex_fk_ret_s = {(r['date'], r['sku']): r for r in existing_daily.get('fk_returns_sku', [])}
    for r in fk_returns_sku_rows:
        ex_fk_ret_s[(r['date'], r['sku'])] = r
    fk_returns_sku_rows = sorted(ex_fk_ret_s.values(), key=lambda r: (r['date'], r['sku']))

    # ── Amazon Orders/Settlement/Returns (SP-API Reports, dashboard memory ──
    # active.md item #57, 2026-07-14). Requests were already fired earlier in
    # this run (_az_request_report, before Flipkart/Meesho processing) —
    # this just checks/downloads whatever's pending now, after that
    # processing time has given Amazon a real chance to finish (2026-07-15).
    # Persisted per-order into az_orders_daily/az_returns_daily/az_settlement
    # (keyed by order_id, full history — no window, see the merge below) so
    # settlement/return data that arrives weeks after the order can still
    # find and update it. Every failure here is recorded into
    # _run_errors/_run_warnings (Golden Rule 29 — no silent errors) rather
    # than only printed to the console.
    try:
        az_orders_result = _az_poll_report(db, 'orders')
    except Exception as _e:
        az_orders_result = {'status': 'failed', 'content': None, 'range_end': None, 'warnings': [], 'errors': [f"AZ orders: acquisition crashed — {_e}"]}
    for _w in az_orders_result['warnings']:
        _run_warnings.append({'file': 'amazon_orders_api', 'type': 'AMAZON', 'reason': _w})
    for _e in az_orders_result['errors']:
        _run_errors.append({'file': 'amazon_orders_api', 'type': 'AMAZON', 'reason': _e})

    az_orders_new = []
    if az_orders_result['content']:
        try:
            _az_m, _az_s, az_orders_new, _az_new_last = process_az_orders_report(
                az_orders_result['content'], get_config(db, 'az_orders_last_date', '2026-01-01'))
            az_end = az_orders_result.get('range_end') or _az_new_last
            if az_end > get_config(db, 'az_orders_last_date', '2026-01-01'):
                set_config(db, 'az_orders_last_date', az_end)
        except Exception as _e:
            _run_errors.append({'file': 'amazon_orders_api', 'type': 'AMAZON', 'reason': f"AZ Orders: parse failed — {_e}"})

    try:
        az_returns_result = _az_poll_report(db, 'returns')
    except Exception as _e:
        az_returns_result = {'status': 'failed', 'content': None, 'range_end': None, 'warnings': [], 'errors': [f"AZ returns: acquisition crashed — {_e}"]}
    for _w in az_returns_result['warnings']:
        _run_warnings.append({'file': 'amazon_returns_api', 'type': 'AMAZON', 'reason': _w})
    for _e in az_returns_result['errors']:
        _run_errors.append({'file': 'amazon_returns_api', 'type': 'AMAZON', 'reason': _e})

    az_returns_new = []
    if az_returns_result['content']:
        try:
            _az_r, _az_new_last, _az_ri, _az_ai, az_returns_new = process_az_returns_report(
                az_returns_result['content'], get_config(db, 'az_returns_last_date', '2026-01-01'))
            az_end = az_returns_result.get('range_end') or _az_new_last
            if az_end > get_config(db, 'az_returns_last_date', '2026-01-01'):
                set_config(db, 'az_returns_last_date', az_end)
        except Exception as _e:
            _run_errors.append({'file': 'amazon_returns_api', 'type': 'AMAZON', 'reason': f"AZ Returns: parse failed — {_e}"})

    try:
        az_sqp_result = _az_poll_sqp(db)
    except Exception as _e:
        az_sqp_result = {'status': 'failed', 'contents': [], 'warnings': [], 'errors': [f"AZ SQP: acquisition crashed — {_e}"]}
    for _w in az_sqp_result['warnings']:
        _run_warnings.append({'file': 'amazon_sqp_api', 'type': 'AMAZON', 'reason': _w})
    for _e in az_sqp_result['errors']:
        _run_errors.append({'file': 'amazon_sqp_api', 'type': 'AMAZON', 'reason': _e})

    az_sqp_rows = []
    for _content in az_sqp_result['contents']:
        try:
            az_sqp_rows.extend(process_az_search_query_performance(_content))
        except Exception as _e:
            _run_errors.append({'file': 'amazon_sqp_api', 'type': 'AMAZON', 'reason': f"AZ SQP: parse failed — {_e}"})

    try:
        az_catalog_result = _az_poll_catalog(db)
    except Exception as _e:
        az_catalog_result = {'status': 'failed', 'content': None, 'warnings': [], 'errors': [f"AZ catalog: acquisition crashed — {_e}"]}
    for _w in az_catalog_result['warnings']:
        _run_warnings.append({'file': 'amazon_catalog_api', 'type': 'AMAZON', 'reason': _w})
    for _e in az_catalog_result['errors']:
        _run_errors.append({'file': 'amazon_catalog_api', 'type': 'AMAZON', 'reason': _e})

    az_catalog_rows = []
    if az_catalog_result['content']:
        try:
            az_catalog_rows = process_az_catalog_report(az_catalog_result['content'])
            set_config(db, 'az_catalog_last_pulled', TODAY)
        except Exception as _e:
            _run_errors.append({'file': 'amazon_catalog_api', 'type': 'AMAZON', 'reason': f"AZ Catalog: parse failed — {_e}"})

    try:
        az_settlement_result = _az_acquire_settlement(db)
    except Exception as _e:
        az_settlement_result = {'status': 'failed', 'contents': [], 'warnings': [], 'errors': [f"AZ settlement: acquisition crashed — {_e}"]}
    for _w in az_settlement_result['warnings']:
        _run_warnings.append({'file': 'amazon_settlement_api', 'type': 'AMAZON', 'reason': _w})
    for _e in az_settlement_result['errors']:
        _run_errors.append({'file': 'amazon_settlement_api', 'type': 'AMAZON', 'reason': _e})

    # Amazon commonly posts more than one settlement event for the same order
    # over time (e.g. an initial sale-commission settlement, then a separate
    # refund/adjustment settlement weeks later after a return) — fee dicts
    # for the same order_id must be SUMMED across reports/runs, never
    # replaced, or an earlier settlement's contribution is silently lost.
    _AZ_FEE_KEYS = ('settlement', 'commission', 'shipping_fwd', 'tcs', 'fixed_fee')

    def _az_sum_fees(a, b):
        return {k: round(float(a.get(k, 0) or 0) + float(b.get(k, 0) or 0), 2) for k in _AZ_FEE_KEYS}

    # Only advance az_settlement_last_created past reports that actually
    # parsed successfully — _az_acquire_settlement deliberately leaves this
    # watermark untouched so a report that downloads but fails to parse
    # (e.g. unrecognized columns) gets retried next run instead of being
    # silently skipped forever (dashboard memory active.md #57 review finding).
    az_settlement_new = {}
    # order_ids refunded per any settlement report processed this run
    # (active.md item #66, 2026-07-18) -- a set, not a dict, since "was this
    # ever refunded" only needs to be true once and stay true; applied to
    # az_orders_daily_rows below, always overriding the Orders report's
    # 'placed' default.
    az_refunded_new = set()
    _az_settlement_watermark = get_config(db, 'az_settlement_last_created', '')
    for _created_time, _content in az_settlement_result['contents']:
        try:
            _fees_this, _refunded_this = process_az_settlement_report(_content)
            for _oid, _fees in _fees_this.items():
                az_settlement_new[_oid] = _az_sum_fees(az_settlement_new.get(_oid, {}), _fees)
            az_refunded_new |= _refunded_this
            if _created_time > _az_settlement_watermark:
                _az_settlement_watermark = _created_time
        except Exception as _e:
            _run_errors.append({'file': 'amazon_settlement_api', 'type': 'AMAZON', 'reason': f"AZ Settlement: parse failed — {_e}"})
    if _az_settlement_watermark != get_config(db, 'az_settlement_last_created', ''):
        set_config(db, 'az_settlement_last_created', _az_settlement_watermark)

    # Merge into persisted per-order tables (existing_daily already loaded above).
    # No window_start cutoff here (2026-07-15, Jaiswal) -- these tables are the
    # sole source for the all-time Orders Ledger, unlike FK/ME's daily tables
    # which only feed recent-trend charts. Windowing them would silently drop
    # older orders/settlement from the persisted DB on every run.
    ex_az_ord = {r['order_id']: r for r in existing_daily.get('az_orders_daily', [])}
    for r in az_orders_new:
        ex_az_ord[r['order_id']] = r
    # Settlement-derived 'return' status always overrides the Orders
    # report's 'placed' default (active.md item #66, 2026-07-18) -- applied
    # against the FULL persisted order history, not just this run's new
    # orders, since a refund can be settled weeks after the original order's
    # own run already wrote its 'placed' row.
    for _oid in az_refunded_new:
        if _oid in ex_az_ord:
            ex_az_ord[_oid]['status'] = 'return'
    az_orders_daily_rows = list(ex_az_ord.values())

    ex_az_ret = {r['order_id']: r for r in existing_daily.get('az_returns_daily', [])}
    for r in az_returns_new:
        ex_az_ret[r['order_id']] = r
    az_returns_daily_rows = list(ex_az_ret.values())

    ex_az_sett = {r['order_id']: r for r in existing_daily.get('az_settlement', [])}
    for oid, fees in az_settlement_new.items():
        ex_az_sett[oid] = {'order_id': oid, **_az_sum_fees(ex_az_sett.get(oid, {}), fees)}
    az_settlement_rows = list(ex_az_sett.values())

    # FK/ME per-order registry (active.md item #64, 2026-07-17; rebuilt item
    # #67, 2026-07-19 to be Orders-file-PRIMARY + Payments-file-OVERRIDE for
    # BOTH platforms, and to carry the full set of Ledger financial fields,
    # not just sku/status). Previously FK's registry was seeded ONLY from
    # fk_pay_order_rows (the Payments file) -- since Payments files land far
    # less often than the Orders/Fulfilment file, a real placed order with
    # no Payments match yet never appeared in the registry, the status
    # filter, stock decrement, OR the Ledger at all. Persisted unbounded so
    # a return/settlement arriving long after the original order can still
    # be resolved, and so the Ledger has a complete, growing history.
    # Deliberately does NOT overwrite an existing entry's sku/order_date
    # with blank/missing values -- a later run re-seeing the same order_id
    # with incomplete data must never erase a previously-good mapping.
    #
    # Step 1 (FK) -- Orders file establishes identity + qty. status starts
    # 'placed' (FK's Orders/Fulfilment report has no status column at all,
    # confirmed active.md item #66 -- no cancellation signal to trust from
    # it). Overridden below by fk_order_statuses_new once Payments resolves it.
    ex_fk_sku_idx = {r['order_id']: r for r in existing_daily.get('fk_order_sku_index', [])}
    for r in fk_ord_order_rows:
        oid = r.get('order_id', '')
        if oid and r.get('sku'):
            prior = ex_fk_sku_idx.get(oid, {})
            ex_fk_sku_idx[oid] = {
                **prior,
                'order_id': oid, 'sku': r['sku'], 'order_date': r.get('order_date', ''),
                'qty': r.get('qty', 1),
                'status': prior.get('status', 'placed'),
            }
    # Step 2 (FK) -- Payments file overrides financial fields once matched.
    # ALWAYS wins over the Orders-file baseline (a Payments row only exists
    # once an order reached a final outcome, confirmed against real live
    # samples). Creates the registry entry even if no Orders-file row has
    # been seen yet, so settlement data is never dropped waiting for the
    # other file to catch up.
    _FK_FIN_KEYS = ('gmv', 'settlement', 'commission', 'fixed_fee', 'collection_fee',
                     'shipping_fwd', 'shipping_rev', 'gst_on_fees', 'tcs', 'tds',
                     'penalty', 'zone', 'is_shopsy')
    for r in fk_pay_order_rows:
        oid = r.get('order_id', '')
        if not oid:
            continue
        entry = ex_fk_sku_idx.setdefault(oid, {'order_id': oid, 'status': 'placed'})
        entry['sku']        = entry.get('sku') or r.get('sku', '')
        entry['order_date'] = entry.get('order_date') or r.get('order_date', '')
        for k in _FK_FIN_KEYS:
            entry[k] = r.get(k, entry.get(k, 0))
    for oid, canon in fk_order_statuses_new.items():
        if oid in ex_fk_sku_idx:
            ex_fk_sku_idx[oid]['status'] = canon
    fk_order_sku_index_rows = list(ex_fk_sku_idx.values())

    # Step 1 (ME) -- Orders file already was identity-primary before today,
    # unchanged. 'cancelled' if the Orders file's own status says CANCELLED/
    # LOST (reliable, final -- a cancelled-before-shipment order never
    # reaches the Payments file, confirmed against a real live sample).
    # Otherwise 'placed' -- the Orders file's Delivered/RTO/In-Transit guess
    # is NOT trusted (confirmed unreliable, active.md item #66), only
    # Payments' Live Order Status is, applied as an override below.
    ex_me_sku_idx = {r['order_id']: r for r in existing_daily.get('me_order_sku_index', [])}
    for r in me_order_rows:
        oid = r.get('order_id', '')
        if oid and r.get('sku_name'):
            prior = ex_me_sku_idx.get(oid, {})
            fallback = 'cancelled' if r.get('status') == 'Cancelled' else prior.get('status', 'placed')
            ex_me_sku_idx[oid] = {
                **prior,
                'order_id': oid, 'sku': r.get('sku', prior.get('sku', '')), 'sku_name': r['sku_name'],
                'order_date': r.get('order_date', ''), 'qty': r.get('qty', 1),
                'status': fallback,
            }
    # Step 2 (ME) -- Payments file overrides the real settlement amount
    # (Meesho's Payments file only breaks out settlement, not a fee
    # itemization the way FK's does) and canonical status once matched.
    for oid, sett in me_order_settlements_new.items():
        if oid in ex_me_sku_idx:
            ex_me_sku_idx[oid]['settlement'] = sett
    for oid, canon in me_order_statuses_new.items():
        if oid in ex_me_sku_idx:
            ex_me_sku_idx[oid]['status'] = canon
    me_order_sku_index_rows = list(ex_me_sku_idx.values())

    # Compact per-day-per-status order counts, derived from the two indices
    # above -- feeds the dashboard status filter (active.md item #67).
    fk_orders_status_daily_rows = _status_daily_rollup(fk_order_sku_index_rows)
    me_orders_status_daily_rows = _status_daily_rollup(me_order_sku_index_rows)

    # Persisted order_id -> AWB (see _DAILY_SCHEMAS comment for why this is
    # needed in addition to the sku indices above). Sourced from this run's
    # fk_order_awb_index/me_suborder_awb_index (built during FK_RETURNS/
    # ME_RETURNS processing above, ~line 6234) -- only present for orders
    # that actually had a return file row this run, so most runs add few or
    # no rows here; that's expected, not a bug.
    ex_fk_awb_idx = {r['order_id']: r for r in existing_daily.get('fk_order_awb_index', [])}
    for oid, awb in fk_order_awb_index.items():
        if oid and awb:
            ex_fk_awb_idx[oid] = {'order_id': oid, 'awb': awb}
    fk_order_awb_index_rows = list(ex_fk_awb_idx.values())

    ex_me_awb_idx = {r['order_id']: r for r in existing_daily.get('me_order_awb_index', [])}
    for oid, awb in me_suborder_awb_index.items():
        if oid and awb:
            ex_me_awb_idx[oid] = {'order_id': oid, 'awb': awb}
    me_order_awb_index_rows = list(ex_me_awb_idx.values())

    # Persisted order_id -> sku, straight from each platform's own RETURNS
    # report (item #72, 2026-07-21) -- same "only present for orders with an
    # actual return row this run" shape as the AWB indices just above.
    ex_fk_ret_sku_idx = {r['order_id']: r for r in existing_daily.get('fk_return_sku_index', [])}
    for oid, sku in fk_return_sku_index.items():
        if oid and sku:
            ex_fk_ret_sku_idx[oid] = {'order_id': oid, 'sku': sku}
    fk_return_sku_index_rows = list(ex_fk_ret_sku_idx.values())

    ex_me_ret_sku_idx = {r['order_id']: r for r in existing_daily.get('me_return_sku_index', [])}
    for oid, sku in me_suborder_sku_index.items():
        if oid and sku:
            ex_me_ret_sku_idx[oid] = {'order_id': oid, 'sku': sku}
    me_return_sku_index_rows = list(ex_me_ret_sku_idx.values())

    # Sale-triggered stock decrement + return credit-back (active.md item #64,
    # 2026-07-17, Jaiswal: "when an order is placed... that's going to
    # decrease the stock... when we receive return or RTO... increase the
    # stock if intact"). Runs on genuinely new-this-run order lines only
    # (fk_orders_sku_new/me_order_rows/az_orders_new -- NOT me_daily_new,
    # which is a rolling recompute with no watermark of its own, see
    # _process_sale_stock_decrement's own docstring for why that distinction
    # is load-bearing), same "new rows this run" idempotency the Orders
    # Ledger already relies on. Never allowed to
    # abort the pipeline run -- both are wrapped so a Firestore hiccup here
    # shows up in _run_errors/Discord (Golden Rule 29) instead of failing
    # the whole run's DB save.
    try:
        _stock_summary = _process_sale_stock_decrement(fk_orders_sku_new, me_order_rows, az_orders_new)
        for _e in _stock_summary.get('errors', []):
            _run_errors.append({'file': 'stock_decrement', 'type': 'STOCK', 'reason': _e})
    except Exception as _e:
        _stock_summary = {'resolved': 0, 'unresolved': 0, 'movements': 0, 'no_bom': 0}
        _run_errors.append({'file': 'stock_decrement', 'type': 'STOCK', 'reason': f"Stock decrement crashed — {_e}"})

    try:
        from firestore_connector import load_product_master_sku_index, load_stock_sku_overrides, load_final_boms
        _pm_sku_index   = load_product_master_sku_index()
        _sku_overrides  = load_stock_sku_overrides()
        _final_boms     = load_final_boms()
        _return_summary, stock_return_credits_rows = _process_return_stock_credit(
            existing_daily.get('stock_return_credits', []),
            fk_order_sku_index_rows, me_order_sku_index_rows, az_orders_daily_rows,
            fk_order_awb_index_rows, me_order_awb_index_rows, az_returns_daily_rows,
            _pm_sku_index, _sku_overrides, _final_boms)
        for _e in _return_summary.get('errors', []):
            _run_errors.append({'file': 'stock_return_credit', 'type': 'STOCK', 'reason': _e})
    except Exception as _e:
        stock_return_credits_rows = existing_daily.get('stock_return_credits', [])
        _return_summary = {'orders_credited': 0, 'movements': 0, 'errors': []}
        _run_errors.append({'file': 'stock_return_credit', 'type': 'STOCK', 'reason': f"Return credit crashed — {_e}"})

    # Keyed by (period_type, asin, search_query, period_start) -- period_type
    # is included even though only WEEK exists today so a future MONTH/QUARTER
    # period starting on the same date as a WEEK can never collide and
    # silently overwrite it. A re-requested/overlapping period (shouldn't
    # normally happen given the watermark, but a manual config edit could
    # cause one) simply replaces that period's row rather than duplicating it.
    _az_sqp_key = lambda r: (r.get('period_type', ''), r.get('asin', ''), r.get('search_query', ''), r.get('period_start', ''))
    ex_az_sqp = {_az_sqp_key(r): r for r in existing_daily.get('az_search_terms', [])}
    for r in az_sqp_rows:
        ex_az_sqp[_az_sqp_key(r)] = r
    az_search_terms_rows = list(ex_az_sqp.values())

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

    # ── Orders Ledger ─────────────────────────────────────────────────────────
    # Bounded Ledger send-list (active.md item #67, 2026-07-19) -- only
    # orders actually touched THIS run (new Orders-file lines + orders whose
    # Payments match just arrived, even if the order was first inserted on
    # an earlier run), never the full persisted registry. upsert_rows
    # re-fetches and diffs the whole sheet on every call -- sending the full
    # unbounded history every run would turn every run into a full-sheet
    # rewrite as the registry grows, burning Sheets API quota on rows that
    # haven't actually changed. Registry status (lowercase canonical, used
    # by the dashboard filter) is translated to the Ledger's own TitleCase
    # vocabulary here, at the boundary -- the registry itself never stores
    # the Ledger label.
    _REGISTRY_TO_LEDGER_STATUS = {
        'delivered': 'Delivered', 'rto': 'RTO', 'return': 'Returned-Customer',
        'cancelled': 'Cancelled', 'placed': 'In-Transit',
    }
    def _ledger_touched_rows(registry, platform, new_order_rows, *extra_oid_sources):
        oids = {r.get('order_id', '') for r in new_order_rows if r.get('order_id')}
        for src in extra_oid_sources:
            oids |= set(src)
        rows = []
        for oid in oids:
            if oid in registry:
                row = dict(registry[oid])
                row['status']   = _REGISTRY_TO_LEDGER_STATUS.get(row.get('status', 'placed'), 'In-Transit')
                row['platform'] = platform  # not stored on the registry itself -- constant per call
                rows.append(row)
        return rows

    fk_ledger_source_rows = _ledger_touched_rows(
        ex_fk_sku_idx, 'FK', fk_ord_order_rows, {r.get('order_id', '') for r in fk_pay_order_rows}
    )
    me_ledger_source_rows = _ledger_touched_rows(
        ex_me_sku_idx, 'ME', me_order_rows, me_order_statuses_new, me_order_settlements_new
    )

    if fk_ledger_source_rows:
        try:
            from sheets_connector import get_or_create_ledger, fetch_return_receipts, upsert_rows, FINAL_STATUSES

            ledger_sheet_id = get_or_create_ledger(
                lambda k: get_config(db, k),
                lambda k, v: set_config(db, k, v),
            )

            # Fetch return receipts for condition lookup
            try:
                receipts = fetch_return_receipts()
                print(f"  [Ledger] Return receipts loaded: {len(receipts)} entries")
            except Exception as _e:
                receipts = {}
                print(f"  [Ledger] Warning: could not load return receipts — {_e}")
                _run_warnings.append({'file': 'fk_return_receipts', 'type': 'FK', 'reason': f"could not load return receipts: {_e}",
                                       'impact': "FK Orders Ledger built this run without return-condition data — return rows may show blank condition until a future run recovers it"})

            # Enrich ledger rows with COGS from fk_skus (existing db data)
            cogs_by_sku = {
                r['sku_id']: float(r.get('cogs', 0) or 0)
                for r in db.get('fk_skus', [])
                if r.get('sku_id')
            }
            for row in fk_ledger_source_rows:
                row['cogs'] = cogs_by_sku.get(row.get('sku', ''), 0.0)

            # Packaging config -- packaging_cost_per_order stays a flat
            # per-order figure (unchanged, Jaiswal didn't ask to change this
            # one). always_lost_cost/box_sticker_cost/chain_cost are REAL,
            # from Materials data (active.md #46) -- used only for the
            # return-loss calculation inside build_fk_ledger_rows.
            from firestore_connector import fetch_packaging_costs
            pkg_cfg = {
                'packaging_cost_per_order': float(get_config(db, 'packaging_cost_per_order', None) or 12.0),
                'bubble_wrap_cost':         float(get_config(db, 'bubble_wrap_cost', None) or 2.0),
                'bubble_wrap_cutoff':       get_config(db, 'bubble_wrap_cutoff', None) or '2026-05-01',
                **fetch_packaging_costs(),
            }

            # Build ads apportionment index: {sku: {date: {ad_spend, orders}}}
            ads_apport = {}
            for r in db.get('fk_orders_sku', []):
                sku, dt = r.get('sku', ''), r.get('date', '')
                if sku and dt:
                    ads_apport.setdefault(sku, {})[dt] = {'orders': r.get('orders', 1), 'ad_spend': 0.0}
            for r in db.get('fk_ads_sku', []):
                sku, dt = r.get('sku', ''), r.get('date', '')
                if sku and dt and sku in ads_apport and dt in ads_apport[sku]:
                    ads_apport[sku][dt]['ad_spend'] = float(r.get('ad_spend', 0) or 0)

            # Build ledger rows
            ledger_rows = build_fk_ledger_rows(
                fk_ledger_source_rows,
                db.get('fk_claims', []),
                receipts,
                pkg_cfg,
                ads_apport,
                fk_order_awb_index,
            )

            # Write to Google Sheet
            inserted, updated = upsert_rows(ledger_sheet_id, ledger_rows)
            print(f"  [Ledger] FK: {inserted} inserted, {updated} updated in Orders Ledger")

            # Derive enrichment columns and write back to fk_skus in db
            enrichment = derive_fk_sku_enrichment(ledger_rows)
            for r in db.get('fk_skus', []):
                sid = r.get('sku_id', '')
                if sid in enrichment:
                    r.update(enrichment[sid])
            print(f"  [Ledger] fk_skus enriched: {len(enrichment)} SKUs with return_rate, rto_rate, net_pl")

        except Exception as _e:
            import traceback as _tb2
            print(f"  [Ledger] ERROR building FK ledger — {_e}")
            _tb2.print_exc()
            # Was silent (not appended) unlike the AZ ledger's identical
            # crash handler below -- a real inconsistency, fixed active.md
            # item #70, 2026-07-20.
            _run_errors.append({'file': 'fk_ledger', 'type': 'FK', 'reason': f"FK Ledger build failed — {_e}",
                                 'impact': "Flipkart Orders Ledger was not updated this run — new/changed FK orders won't appear in the Ledger sheet until a future run succeeds"})

    if me_ledger_source_rows:
        try:
            from sheets_connector import get_or_create_ledger, fetch_return_receipts, upsert_rows

            ledger_sheet_id = get_or_create_ledger(
                lambda k: get_config(db, k),
                lambda k, v: set_config(db, k, v),
            )

            # Fetch return receipts (may already be in scope from FK block)
            try:
                receipts  # noqa: already fetched above
            except NameError:
                try:
                    receipts = fetch_return_receipts()
                    print(f"  [Ledger] Return receipts loaded: {len(receipts)} entries")
                except Exception as _e:
                    receipts = {}
                    print(f"  [Ledger] Warning: could not load return receipts — {_e}")
                    _run_warnings.append({'file': 'me_return_receipts', 'type': 'ME', 'reason': f"could not load return receipts: {_e}",
                                           'impact': "ME Orders Ledger built this run without return-condition data — return rows may show blank condition until a future run recovers it"})

            # Enrich ledger rows with COGS from me_skus
            cogs_by_me_sku = {
                r['sku_id']: float(r.get('cogs', 0) or 0)
                for r in db.get('me_skus', [])
                if r.get('sku_id')
            }
            for row in me_ledger_source_rows:
                row['cogs'] = cogs_by_me_sku.get(row.get('sku', ''), 0.0)

            from firestore_connector import fetch_packaging_costs
            pkg_cfg = {
                'packaging_cost_per_order': float(get_config(db, 'packaging_cost_per_order', None) or 12.0),
                'bubble_wrap_cost':         float(get_config(db, 'bubble_wrap_cost', None) or 2.0),
                'bubble_wrap_cutoff':       get_config(db, 'bubble_wrap_cutoff', None) or '2026-05-01',
                **fetch_packaging_costs(),
            }

            me_ledger_rows = build_me_ledger_rows(
                me_ledger_source_rows,
                me_return_reason_index,
                db.get('me_claims', []),
                receipts,
                pkg_cfg,
                me_suborder_awb_index,
            )

            inserted, updated = upsert_rows(ledger_sheet_id, me_ledger_rows)
            print(f"  [Ledger] ME: {inserted} inserted, {updated} updated in Orders Ledger")

            enrichment_me = derive_me_sku_enrichment(me_ledger_rows)
            for r in db.get('me_skus', []):
                sid = r.get('sku_id', '')
                if sid in enrichment_me:
                    r.update(enrichment_me[sid])
            print(f"  [Ledger] me_skus enriched: {len(enrichment_me)} SKUs with return_rate, rto_rate, net_pl")

        except Exception as _e:
            import traceback as _tb2
            print(f"  [Ledger] ERROR building ME ledger — {_e}")
            _tb2.print_exc()
            # Was silent (not appended), same inconsistency as the FK ledger
            # handler above -- fixed active.md item #70, 2026-07-20.
            _run_errors.append({'file': 'me_ledger', 'type': 'ME', 'reason': f"ME Ledger build failed — {_e}",
                                 'impact': "Meesho Orders Ledger was not updated this run — new/changed ME orders won't appear in the Ledger sheet until a future run succeeds"})

    _az_ledger_summary = {'ran': False, 'inserted': 0, 'updated': 0}
    if az_orders_daily_rows:
        try:
            from sheets_connector import get_or_create_ledger, fetch_return_receipts, upsert_rows

            ledger_sheet_id = get_or_create_ledger(
                lambda k: get_config(db, k),
                lambda k, v: set_config(db, k, v),
            )

            try:
                receipts  # noqa: already fetched above from the FK/ME blocks
            except NameError:
                try:
                    receipts = fetch_return_receipts()
                    print(f"  [Ledger] Return receipts loaded: {len(receipts)} entries")
                except Exception as _e:
                    receipts = {}
                    print(f"  [Ledger] Warning: could not load return receipts — {_e}")
                    _run_warnings.append({'file': 'az_return_receipts', 'type': 'AMAZON', 'reason': f"could not load return receipts: {_e}",
                                           'impact': "AZ Orders Ledger built this run without return-condition data — return rows may show blank condition until a future run recovers it"})

            # COGS: Amazon has no per-SKU cost table of its own yet (az_monthly
            # is a monthly aggregate only, no SKU rows) — best-effort join
            # against fk_skus/me_skus by sku_id, on the assumption the same
            # product design may be listed under the same SKU string across
            # platforms. NOT confirmed against real data — flagged in
            # dashboard memory #57 as an open item for Jaiswal to weigh in on.
            cogs_by_sku = {}
            for _tbl in ('fk_skus', 'me_skus'):
                for r in db.get(_tbl, []):
                    if r.get('sku_id') and r.get('sku_id') not in cogs_by_sku:
                        cogs_by_sku[r['sku_id']] = float(r.get('cogs', 0) or 0)
            az_order_rows = [dict(r) for r in az_orders_daily_rows]
            for row in az_order_rows:
                row['cogs'] = cogs_by_sku.get(row.get('sku', ''), 0.0)

            az_settlement_fees = {r['order_id']: {k: r.get(k, 0) for k in
                                  ('settlement', 'commission', 'shipping_fwd', 'fixed_fee')}
                                  for r in az_settlement_rows}
            az_return_reason_index = {r['order_id']: r['return_reason']
                                      for r in az_returns_daily_rows if r.get('return_reason')}
            az_order_awb_index = {r['order_id']: r['tracking_id']
                                  for r in az_returns_daily_rows if r.get('tracking_id')}

            from firestore_connector import fetch_packaging_costs
            pkg_cfg = {
                'packaging_cost_per_order': float(get_config(db, 'packaging_cost_per_order', None) or 12.0),
                'bubble_wrap_cost':         float(get_config(db, 'bubble_wrap_cost', None) or 2.0),
                'bubble_wrap_cutoff':       get_config(db, 'bubble_wrap_cutoff', None) or '2026-05-01',
                **fetch_packaging_costs(),
            }

            az_ledger_rows = build_az_ledger_rows(
                az_order_rows,
                az_settlement_fees,
                az_return_reason_index,
                receipts,
                pkg_cfg,
                az_order_awb_index,
            )

            inserted, updated = upsert_rows(ledger_sheet_id, az_ledger_rows)
            print(f"  [Ledger] AZ: {inserted} inserted, {updated} updated in Orders Ledger")
            _az_ledger_summary = {'ran': True, 'inserted': inserted, 'updated': updated}

        except Exception as _e:
            import traceback as _tb2
            print(f"  [Ledger] ERROR building AZ ledger — {_e}")
            _tb2.print_exc()
            _run_errors.append({'file': 'amazon_ledger', 'type': 'AMAZON', 'reason': f"AZ Ledger build failed — {_e}"})

    # ── Amazon monthly rollup (derived from az_orders_daily, no live call) ────
    db['az_monthly'] = _az_monthly_rollup(az_orders_daily_rows, az_returns_daily_rows, az_settlement_rows)

    # ── Amazon Discord notification — fires every run (restored 2026-07-15, ──
    # Jaiswal asked for it back after the old process_az_monthly-era one was
    # removed as dead code alongside it).
    _az_amazon_errors   = [e['reason'] for e in _run_errors   if e.get('type') == 'AMAZON']
    _az_amazon_warnings = [w['reason'] for w in _run_warnings if w.get('type') == 'AMAZON']
    send_discord_az_notification({
        'orders_req':         _az_orders_req.get('status', '?'),
        'orders_poll':        az_orders_result.get('status', '?'),
        'orders_rows':        len(az_orders_new),
        'returns_req':        _az_returns_req.get('status', '?'),
        'returns_poll':       az_returns_result.get('status', '?'),
        'returns_rows':       len(az_returns_new),
        'sqp_req':            _az_sqp_req.get('status', '?'),
        'sqp_poll':           az_sqp_result.get('status', '?'),
        'sqp_rows':           len(az_sqp_rows),
        'catalog_req':        _az_catalog_req.get('status', '?'),
        'catalog_poll':       az_catalog_result.get('status', '?'),
        'catalog_rows':       len(az_catalog_rows),
        'settlement_status':  az_settlement_result.get('status', '?'),
        'settlement_rows':    len(az_settlement_new),
        'ledger_ran':         _az_ledger_summary['ran'],
        'ledger_inserted':    _az_ledger_summary['inserted'],
        'ledger_updated':     _az_ledger_summary['updated'],
        'errors':             _az_amazon_errors,
        'warnings':           _az_amazon_warnings,
    })

    # ── Save DB ───────────────────────────────────────────────────────────────
    save_db(db, DB_SUMMARY_PATH)
    save_daily_csv({
        'fk_daily':        fk_daily_rows,
        'me_daily':        me_daily_rows,
        'fk_orders_daily': fk_orders_daily_rows,
        'fk_orders_sku':   fk_orders_sku_rows,
        'fk_returns_daily': fk_returns_daily_rows,
        'fk_returns_sku':  fk_returns_sku_rows,
        'az_orders_daily':  az_orders_daily_rows,
        'az_returns_daily': az_returns_daily_rows,
        'az_settlement':    az_settlement_rows,
        'az_search_terms':  az_search_terms_rows,
        'fk_order_sku_index': fk_order_sku_index_rows,
        'me_order_sku_index': me_order_sku_index_rows,
        'fk_orders_status_daily': fk_orders_status_daily_rows,
        'me_orders_status_daily': me_orders_status_daily_rows,
        'fk_order_awb_index': fk_order_awb_index_rows,
        'me_order_awb_index': me_order_awb_index_rows,
        'fk_return_sku_index': fk_return_sku_index_rows,
        'me_return_sku_index': me_return_sku_index_rows,
        'stock_return_credits': stock_return_credits_rows,
    }, DB_DAILY_PATH)
    save_keywords_csv(kw_rows, DB_KEYWORDS_PATH)

    # ── FK Ads campaign / SKU / keyword / placement / search / order data ────
    _any_fk_ads = any([fk_ads_daily_rows, fk_ads_sku_rows, fk_ads_kw_rows,
                        fk_ads_placements_rows, fk_ads_overall_rows,
                        fk_ads_search_rows, fk_ads_order_rows])
    if _any_fk_ads:
        ex = load_fk_ads_db(DB_FK_ADS_PATH)

        def _upsert(existing, new_rows, *key_fields):
            _k = lambda r: tuple(_key_norm(r.get(f, '')) for f in key_fields)
            d = {_k(r): r for r in existing}
            for r in new_rows:
                d[_k(r)] = r
            return sorted(d.values(), key=_k)

        tables = {
            'fk_ads_daily':      _upsert(ex['fk_ads_daily'],      fk_ads_daily_rows,      'date', 'campaign_id'),
            'fk_ads_sku':        _upsert(ex['fk_ads_sku'],        fk_ads_sku_rows,        'date', 'campaign_id', 'sku_id'),
            'fk_ads_kw':         _upsert(ex['fk_ads_kw'],         fk_ads_kw_rows,         'date', 'campaign_id', 'keyword', 'match_type'),
            'fk_ads_placements': _upsert(ex['fk_ads_placements'], fk_ads_placements_rows, 'date', 'campaign_id', 'placement_type'),
            'fk_ads_overall':    _upsert(ex['fk_ads_overall'],    fk_ads_overall_rows,    'date', 'campaign_id', 'sku_id'),
            'fk_ads_search':     _upsert(ex['fk_ads_search'],     fk_ads_search_rows,     'date', 'campaign_id', 'query'),
            'fk_ads_order_items':_upsert(ex['fk_ads_order_items'],fk_ads_order_rows,      'date', 'order_id'),
        }
        save_fk_ads_csv(tables, DB_FK_ADS_PATH)

    # ── ME Ads campaign / catalog / master data ───────────────────────────────
    _any_me_ads = any([me_ads_daily_rows, me_ads_catalog_rows, me_ads_master_rows])
    if _any_me_ads:
        ex_me_ads = load_me_ads_db(DB_ME_ADS_PATH)

        def _upsert_me(existing, new_rows, *key_fields):
            _k = lambda r: tuple(_key_norm(r.get(f, '')) for f in key_fields)
            d = {_k(r): r for r in existing}
            for r in new_rows:
                d[_k(r)] = r
            return sorted(d.values(), key=_k)

        me_tables = {
            'me_ads_daily':   _upsert_me(ex_me_ads['me_ads_daily'],   me_ads_daily_rows,   'date', 'campaign_id'),
            'me_ads_catalog': _upsert_me(ex_me_ads['me_ads_catalog'], me_ads_catalog_rows, 'date', 'campaign_id', 'catalog_id'),
            'me_ads_master':  me_ads_master_rows if me_ads_master_rows else ex_me_ads['me_ads_master'],
        }
        save_me_ads_csv(me_tables, DB_ME_ADS_PATH)

    # ── Write CSV data to Firestore (incremental monthly structure) ──────────
    # Each table is split by month — one Firestore document per (table, month).
    # Historical month docs are written once and stay immutable.
    # Only the current month doc is overwritten on each daily run.
    try:
        from firestore_connector import write_csv_content, write_monthly_table

        def _split_by_month(file_path, table_name, date_col=1):
            """Group CSV rows by YYYY_MM. Returns {month_key: csv_string}."""
            header_line = None
            month_chunks = {}
            with open(file_path, encoding='utf-8') as f:
                for raw in f:
                    line = raw.rstrip('\r\n')
                    if not line:
                        continue
                    if line.startswith('__table__'):
                        header_line = line
                        continue
                    parts = line.split(',')
                    if parts[0].strip() != table_name or not header_line:
                        continue
                    if len(parts) > date_col:
                        date_str = parts[date_col].strip().strip('"')
                        if len(date_str) >= 7:
                            mk = date_str[:7].replace('-', '_')
                            month_chunks.setdefault(mk, [header_line]).append(line)
            return {k: '\n'.join(v) + '\n' for k, v in month_chunks.items()}

        # Summary — aggregated snapshot, small (119 KB), full replace is correct
        write_csv_content('summary', DB_SUMMARY_PATH.read_text(encoding='utf-8'))

        # Daily tables — each month written as one document
        _COLLECTION_MAP = {
            'fk_daily':       'rumee_fk_daily',
            'me_daily':       'rumee_me_daily',
            'fk_orders_daily': 'rumee_orders_daily',
            'fk_orders_sku':  'rumee_orders_sku',
            'fk_returns_daily': 'rumee_fk_returns_daily',
            'fk_returns_sku':  'rumee_fk_returns_sku',
            'fk_orders_status_daily': 'rumee_fk_orders_status_daily',
            'me_orders_status_daily': 'rumee_me_orders_status_daily',
        }
        for tname, collection in _COLLECTION_MAP.items():
            for mk, csv in _split_by_month(DB_DAILY_PATH, tname).items():
                write_monthly_table(collection, mk, csv)

        # Amazon (dashboard memory active.md #57) — order_id is the first
        # schema column (needed for the merge-by-order_id above), so the date
        # is at index 2, not the default index 1. az_settlement has no date
        # column at all (a per-order fee table) so it's intentionally NOT
        # pushed here, same as fk_pay_order_rows/me_order_rows are never
        # pushed raw — it's an internal join table for the Ledger builder,
        # not a dashboard-facing collection.
        for tname, collection in [
            ('az_orders_daily',  'rumee_az_orders_daily'),
            ('az_returns_daily', 'rumee_az_returns_daily'),
        ]:
            for mk, csv in _split_by_month(DB_DAILY_PATH, tname, date_col=2).items():
                write_monthly_table(collection, mk, csv)

        # Search Query Performance -- period_start is the 2nd data column
        # (row = [tname, period_type, period_start, ...]), so date_col=2,
        # same convention as az_orders_daily/az_returns_daily above.
        for mk, csv in _split_by_month(DB_DAILY_PATH, 'az_search_terms', date_col=2).items():
            write_monthly_table('rumee_az_search_terms', mk, csv)

        # Keywords — month field is already YYYY-MM at col 1
        for mk, csv in _split_by_month(DB_KEYWORDS_PATH, 'fk_keywords', date_col=1).items():
            write_monthly_table('rumee_keywords', mk, csv)

        # FK Ads — push daily/kw/sku/placements/order_items tables by month
        if DB_FK_ADS_PATH.exists():
            for tname, collection in [
                ('fk_ads_daily',       'rumee_fk_ads_daily'),
                ('fk_ads_kw',          'rumee_fk_ads_kw'),
                ('fk_ads_sku',         'rumee_fk_ads_sku'),
                ('fk_ads_placements',  'rumee_fk_ads_placements'),
                ('fk_ads_order_items', 'rumee_fk_ads_order_items'),
            ]:
                for mk, csv in _split_by_month(DB_FK_ADS_PATH, tname).items():
                    write_monthly_table(collection, mk, csv)

        # Meesho Ads — push daily/catalog tables by month
        if DB_ME_ADS_PATH.exists():
            for tname, collection in [
                ('me_ads_daily',   'rumee_me_ads_daily'),
                ('me_ads_catalog', 'rumee_me_ads_catalog'),
            ]:
                for mk, csv in _split_by_month(DB_ME_ADS_PATH, tname).items():
                    write_monthly_table(collection, mk, csv)

        # Amazon monthly — push az_monthly rows to rumee_az_daily by month
        for mk, csv in _split_by_month(DB_SUMMARY_PATH, 'az_monthly', date_col=1).items():
            write_monthly_table('rumee_az_daily', mk, csv)

        # Amazon catalog — full-snapshot doc, not CSV-backed (rows come straight
        # from the parsed report, no local file), so this bypasses write_monthly_table.
        # Only writes when a new snapshot actually completed this run (most runs
        # won't have one — see _az_request_catalog's 7-day watermark). Replaces
        # the one-off push_az_catalog_firestore.py script's manual write with the
        # same doc shape (dashboard memory active.md item #17).
        if az_catalog_rows:
            from firestore_connector import get_db as _az_get_fdb
            _az_mk = TODAY[:7].replace('-', '_')
            _az_get_fdb().collection('rumee_az_catalog').document(_az_mk).set({
                'month':     TODAY[:7],
                'pulled_on': TODAY,
                'source':    'SP-API GET_MERCHANT_LISTINGS_ALL_DATA',
                'rows':      az_catalog_rows,
            })

        # Amazon settlement — az_settlement itself has no date column (a
        # per-order fee table, see its DB_TABLES comment), so it can't go
        # through _split_by_month like every other stream. order_date is
        # joined in fresh here, at push time only, from az_orders_daily_rows
        # (already in scope) -- the local az_settlement table/CSV and its
        # order_id-keyed merge logic (used by the Orders Ledger) are left
        # completely untouched. A settlement row with no matching order yet
        # (fee report can arrive before the order row syncs) is skipped this
        # push and picked up automatically once the order exists.
        if az_settlement_rows:
            _az_order_dates    = {r['order_id']: r.get('order_date', '') for r in az_orders_daily_rows}
            _az_sett_by_month  = {}
            for _r in az_settlement_rows:
                _od = _az_order_dates.get(_r['order_id'], '')
                if not _od:
                    continue
                _mk = _od[:7].replace('-', '_')
                _az_sett_by_month.setdefault(_mk, []).append({**_r, 'order_date': _od})
            if _az_sett_by_month:
                import csv as _az_csv, io as _az_io
                _az_sett_fields = ['order_id', 'order_date', 'settlement', 'commission',
                                    'shipping_fwd', 'tcs', 'fixed_fee']
                for _mk, _rows in _az_sett_by_month.items():
                    _buf = _az_io.StringIO()
                    _w = _az_csv.DictWriter(_buf, fieldnames=_az_sett_fields)
                    _w.writeheader()
                    _w.writerows(_rows)
                    write_monthly_table('rumee_az_settlement', _mk, _buf.getvalue())

        # Returns Scanner live lookup: Order-ID/AWB -> SKU (dashboard memory
        # active.md item #72, 2026-07-21). PRIORITY ORDER (Jaiswal, revised
        # same day after his own original "orders-only" call): each
        # platform's OWN return report is checked FIRST once a specific
        # return has actually synced (fk_return_sku_index_rows/
        # me_return_sku_index_rows/az_returns_daily_rows' own "sku" column)
        # -- more authoritative since it's tied to the real return event, not
        # an order-placement-time guess. Falls back to Orders-file data
        # (fk_order_sku_index_rows/me_order_sku_index_rows/
        # az_orders_daily_rows) ONLY when this specific order's return hasn't
        # synced yet -- his original reasoning still holds for that fallback:
        # a return can be scanned before its own report lands, so orders-data
        # remains the safety net, never the other way round.
        # Windowed to the last 45 days by ORDER date for the orders-fallback
        # portion only (his call, keeps the published slice small/fast) --
        # returns-sourced entries are added regardless of order age (return
        # volume is naturally much smaller than order volume, no windowing
        # needed there).
        from datetime import timedelta as _rl_timedelta
        from firestore_connector import (write_return_lookup, load_product_master_sku_index_and_variations,
                                          load_stock_sku_overrides)
        _rl_cutoff  = (date.fromisoformat(TODAY) - _rl_timedelta(days=45)).isoformat()
        _rl_cutoff7 = (date.fromisoformat(TODAY) - _rl_timedelta(days=7)).isoformat()

        # Bahubali/OG per order, resolved via Product Master -- the ONLY
        # authoritative source for variation_type (Jaiswal, 2026-07-21: Chain
        # Intact should default to Intact for Bahubali, stay blank for OG).
        # Never guessed from the raw SKU string (CLAUDE.md standing rule:
        # variation_type is human-set, never text-detected). Loaded fresh
        # here (one combined read, not two -- see
        # load_product_master_sku_index_and_variations's own docstring)
        # rather than trusting _pm_sku_index/_sku_overrides from the
        # return-credit step above to still be in scope -- that step's own
        # try/except could leave them undefined if it failed, and this whole
        # Firestore-push block would then NameError and skip every push
        # below it (az_settlement, alltime, ...), not just this one. On any
        # failure here, every order just falls back to is_bahubali=False --
        # identical to today's existing default-blank Chain Intact behavior,
        # not a regression.
        try:
            _rl_pm_sku_index, _rl_pm_var_types = load_product_master_sku_index_and_variations()
            _rl_sku_overrides = load_stock_sku_overrides()
        except Exception as _rl_e:
            print(f"Warning: could not load Product Master data for Chain-Intact default: {_rl_e}")
            _rl_pm_sku_index, _rl_sku_overrides, _rl_pm_var_types = {}, {}, {}

        def _rl_is_bahubali(platform, raw_sku_for_join):
            pm_id, _ = _resolve_order_sku(platform, raw_sku_for_join, _rl_pm_sku_index, _rl_sku_overrides)
            if not pm_id:
                return False
            return (_rl_pm_var_types.get(pm_id, '') or '').strip().lower() == 'bahubali'

        # Every order_id/awb/order_date is explicitly str()'d before use as a
        # dict key or a cutoff comparison -- FIXED 2026-07-21 after a real
        # live failure: some of these source tables (built from pandas CSVs
        # read without dtype=str) can carry a bare numeric order_id as a
        # Python float (e.g. a pure-digit Meesho suborder number pandas
        # auto-inferred as float64), which throws a TypeError the instant
        # it's used as a Firestore map key (protobuf map keys must be
        # strings) or compared against the string date cutoff below.
        # Confirmed via a real GitHub Actions run: "Warning: could not write
        # order_sku_lookup: '<' not supported between instances of 'float'
        # and 'str'" -- reproduced locally against google-cloud-firestore's
        # own encoder with a synthetic float dict key before applying this
        # fix, not guessed.
        def _rl_str(v):
            return str(v) if v is not None else ''

        # Unwindowed (full-history) order lookups, built once -- used by BOTH
        # steps below: Step 1 filters these by the 45-day cutoff, Step 2 uses
        # them to find each return's real order_date (for the 60-day cap) and,
        # for Meesho specifically, the real sku_name (for correct Bahubali
        # resolution) regardless of whether that order passed Step 1's filter.
        _rl_all_fk = {_rl_str(r['order_id']): r for r in fk_order_sku_index_rows if r.get('order_id')}
        _rl_all_me = {_rl_str(r['order_id']): r for r in me_order_sku_index_rows if r.get('order_id')}
        _rl_all_az = {_rl_str(r['order_id']): r for r in az_orders_daily_rows if r.get('order_id')}

        # Step 1: Orders-side base (45-day window) -- the fallback layer.
        _rl_by_order = {}
        for _oid, r in _rl_all_fk.items():
            if r.get('sku') and _rl_str(r.get('order_date', '')) >= _rl_cutoff:
                _rl_by_order[_oid] = {'platform': 'Flipkart', 'sku': r['sku'], 'order_date': _rl_str(r['order_date']),
                                       'is_bahubali': _rl_is_bahubali('flipkart', r['sku']), 'source': 'order'}
        for _oid, r in _rl_all_me.items():
            if r.get('sku') and _rl_str(r.get('order_date', '')) >= _rl_cutoff:
                _rl_by_order[_oid] = {'platform': 'Meesho', 'sku': r['sku'],
                                       'sku_name': r.get('sku_name', ''), 'order_date': _rl_str(r['order_date']),
                                       'is_bahubali': _rl_is_bahubali('meesho', r.get('sku_name', '')), 'source': 'order'}
        for _oid, r in _rl_all_az.items():
            if r.get('sku') and _rl_str(r.get('order_date', '')) >= _rl_cutoff:
                _rl_by_order[_oid] = {'platform': 'Amazon', 'sku': r['sku'], 'order_date': _rl_str(r['order_date']),
                                       'is_bahubali': _rl_is_bahubali('amazon', r['sku']), 'source': 'order'}

        # Step 2: Returns-side overlay -- takes priority over Step 1, capped
        # at 60 days (Jaiswal, 2026-07-22 -- initially 1 year, narrowed to 60
        # same day since 365 wasn't actually needed -- still a separate,
        # slightly wider cutoff than the 45-day orders-fallback window, so
        # the published doc has a hard ceiling instead of growing forever).
        # Age is checked against the order's REAL order_date via the
        # unwindowed _rl_all_* maps above, not Step 1's already-filtered
        # result -- a return whose order fell outside the 45-day window must
        # still be checked against the 60-day cap correctly, not skipped or
        # wrongly kept just because Step 1 didn't have it.
        #
        # Meesho's returns-side SKU is the short internal code (me_sku_id()),
        # NOT sku_name -- the only field that actually resolves to Product
        # Master (same join precedent as _process_sale_stock_decrement) --
        # so its is_bahubali is resolved from the matching order's REAL
        # sku_name (via _rl_all_me, unwindowed) rather than the wrong join
        # key. FIXED 2026-07-22 (independent finding from a code review):
        # previously this carried over is_bahubali from Step 1's already-
        # windowed result, which meant a Meesho return whose order fell
        # outside the 45-day window always showed is_bahubali=False even for
        # a genuine Bahubali product, since there was nothing to carry over
        # from. Using the unwindowed order data fixes that regardless of
        # order age. FK/Amazon don't have this split (their own "sku" field
        # is the same seller SKU in both orders and returns reports), so
        # those recompute directly against the returns-side sku either way.
        _rl_cutoff_returns = (date.fromisoformat(TODAY) - _rl_timedelta(days=60)).isoformat()

        for r in fk_return_sku_index_rows:
            if r.get('order_id') and r.get('sku'):
                _oid = _rl_str(r['order_id'])
                _order_row = _rl_all_fk.get(_oid)
                _odate = _rl_str(_order_row.get('order_date', '')) if _order_row else ''
                if _odate and _odate < _rl_cutoff_returns:
                    continue  # order confirmed older than 60 days -- drop, keeps the doc bounded
                _rl_by_order[_oid] = {'platform': 'Flipkart', 'sku': r['sku'], 'order_date': _odate,
                                       'is_bahubali': _rl_is_bahubali('flipkart', r['sku']), 'source': 'return'}
        for r in me_return_sku_index_rows:
            if r.get('order_id') and r.get('sku'):
                _oid = _rl_str(r['order_id'])
                _order_row = _rl_all_me.get(_oid)
                _odate = _rl_str(_order_row.get('order_date', '')) if _order_row else ''
                if _odate and _odate < _rl_cutoff_returns:
                    continue
                _me_sku_name = _order_row.get('sku_name', '') if _order_row else ''
                _rl_by_order[_oid] = {'platform': 'Meesho', 'sku': r['sku'], 'order_date': _odate,
                                       'is_bahubali': _rl_is_bahubali('meesho', _me_sku_name), 'source': 'return'}
        for r in az_returns_daily_rows:
            if r.get('order_id') and r.get('sku'):
                _oid = _rl_str(r['order_id'])
                _order_row = _rl_all_az.get(_oid)
                # az_returns_daily rows carry their own return_date, unlike
                # FK/ME's returns-side indices -- used as a fallback age
                # signal only when the order itself isn't in az_orders_daily
                # (rare: a return synced without its order ever being seen).
                _odate = _rl_str(_order_row.get('order_date', '')) if _order_row else _rl_str(r.get('return_date', ''))
                if _odate and _odate < _rl_cutoff_returns:
                    continue
                _rl_by_order[_oid] = {'platform': 'Amazon', 'sku': r['sku'], 'order_date': _odate,
                                       'is_bahubali': _rl_is_bahubali('amazon', r['sku']), 'source': 'return'}

        # AWB side: only worth keeping an entry if its order actually made it
        # into the map above (from either step). Amazon's index is recomputed
        # here (not reused from the AZ Ledger block above) since that block
        # is skipped entirely when az_orders_daily_rows is empty --
        # az_returns_daily_rows itself is always defined by this point, so
        # this is a safe, self-contained rebuild rather than depending on
        # another block's local variable.
        _rl_by_awb = {}
        for r in fk_order_awb_index_rows:
            if r.get('awb') and _rl_str(r.get('order_id')) in _rl_by_order:
                _rl_by_awb[_rl_str(r['awb'])] = _rl_str(r['order_id'])
        for r in me_order_awb_index_rows:
            if r.get('awb') and _rl_str(r.get('order_id')) in _rl_by_order:
                _rl_by_awb[_rl_str(r['awb'])] = _rl_str(r['order_id'])
        for r in az_returns_daily_rows:
            if r.get('tracking_id') and _rl_str(r.get('order_id')) in _rl_by_order:
                _rl_by_awb[_rl_str(r['tracking_id'])] = _rl_str(r['order_id'])

        # Per-platform returns-data freshness (Jaiswal: show on the initial
        # screen whether each platform's returns data is current through the
        # last 7 days, so staff understand why a lookup resolved via the
        # returns-priority path vs the orders-fallback path). Uses the same
        # watermark config keys the rest of the pipeline already tracks.
        _rl_freshness = {}
        for _rl_plat, _rl_cfgkey in (('Flipkart', 'fk_returns_last_date'),
                                      ('Meesho',   'me_returns_last_date'),
                                      ('Amazon',   'az_returns_last_date')):
            _rl_last = _rl_str(get_config(db, _rl_cfgkey, '') or '')
            _rl_freshness[_rl_plat] = {'last_synced': _rl_last, 'fresh': bool(_rl_last) and _rl_last >= _rl_cutoff7}

        write_return_lookup(_rl_by_order, _rl_by_awb, window_days=45, freshness=_rl_freshness)

        # Alltime — generated on demand, full replace is correct (not a daily write)
        if DB_ALLTIME_PATH.exists():
            write_csv_content('alltime', DB_ALLTIME_PATH.read_text(encoding='utf-8'))

    except Exception as e:
        print(f"Warning: Firestore CSV write failed: {e}")
        _run_warnings.append({'file': 'firestore', 'type': 'INFRA', 'reason': f"Firestore write failed: {e}"})

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
            _run_warnings.append({'file': 'drive_temp_cleanup', 'type': 'INFRA', 'reason': f"Drive temp file cleanup failed: {e}",
                                   'impact': "cosmetic only — leftover temp files on disk, no data affected"})

    # ── Summary ───────────────────────────────────────────────────────────────
    summary_rows = sum(len(db.get(t, [])) for t in [
        'config', 'fk_monthly', 'me_monthly', 'fk_skus', 'me_skus',
        'me_return_reasons', 'fk_return_reasons', 'fk_pairs', 'az_monthly', 'fk_keywords',
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
    print(f"  ME return reasons: {len(db.get('me_return_reasons', []))}  "
          f"FK return reasons: {len(db.get('fk_return_reasons', []))}  "
          f"FK Keywords: {len(db.get('fk_keywords', []))}  "
          f"FK Pairs: {len(db.get('fk_pairs', []))}")
    print(f"\n  Next steps:")
    print(f"    git add index.html  # CSVs commit to rumee-data via Actions")
    print(f"    git commit -m \"Data update: {TODAY}\"")
    print(f"    git push origin main")
    print(f"{'='*60}\n")

    # ── Generate Firestore insights + review completed tasks ─────────────────
    generate_insights(db)
    review_completed_tasks(db)
    _fail_count = len(_run_errors)
    _warn_count = len(_run_warnings)
    _run_summary = (f"{len(processed_files)} files processed"
                    + (f", {_fail_count} FAILED" if _fail_count else "")
                    + (f", {_warn_count} warnings" if _warn_count else ""))
    log('RUN_COMPLETE', 'pipeline', _run_summary)
    if _run_errors:
        for _fe in _run_errors:
            log('FAIL_SUMMARY', _fe['file'], _fe['reason'])
    flush_log()

    # ── Write pipeline run log + gap detection ────────────────────────────────
    try:
        import json as _json_rl
        from datetime import date as _gdate, timedelta as _gtd

        def _find_gaps(date_strings):
            """Gaps in a sorted date sequence. Returns [{from, to, missing_days}]."""
            _ds = sorted(set(str(s)[:10] for s in date_strings
                             if s and len(str(s)) >= 10))
            if len(_ds) < 2:
                return []
            _gaps = []
            for _i in range(len(_ds) - 1):
                try:
                    _d1 = _gdate.fromisoformat(_ds[_i])
                    _d2 = _gdate.fromisoformat(_ds[_i + 1])
                    _n  = (_d2 - _d1).days - 1
                    if _n > 0:
                        _gaps.append({
                            'from':         str(_d1 + _gtd(1)),
                            'to':           str(_d2 - _gtd(1)),
                            'missing_days': _n,
                        })
                except (ValueError, TypeError):
                    continue
            return _gaps

        def _find_month_gaps(month_keys):
            """Missing months (YYYY-MM) within the range present in month_keys."""
            _ms = sorted(set(str(m)[:7] for m in month_keys
                             if m and len(str(m)) >= 7))
            if len(_ms) < 2:
                return []
            _gaps = []
            for _i in range(len(_ms) - 1):
                try:
                    _y1, _mo1 = int(_ms[_i][:4]),   int(_ms[_i][5:7])
                    _y2, _mo2 = int(_ms[_i+1][:4]), int(_ms[_i+1][5:7])
                    _total = (_y2 - _y1) * 12 + (_mo2 - _mo1)
                    for _skip in range(1, _total):
                        _mo = _mo1 + _skip
                        _y  = _y1 + (_mo - 1) // 12
                        _mo = ((_mo - 1) % 12) + 1
                        _gaps.append({'from': f'{_y:04d}-{_mo:02d}-01',
                                      'to':   f'{_y:04d}-{_mo:02d}-28',
                                      'missing_days': 28, 'is_month': True,
                                      'month': f'{_y:04d}-{_mo:02d}'})
                except (ValueError, TypeError):
                    continue
            return _gaps

        # ── Date sources for gap detection ────────────────────────────────────
        _me_views_dates = [r['date'] for r in db.get('me_views', []) if r.get('date')]
        _me_views_last  = max(_me_views_dates) if _me_views_dates else None

        _fk_daily_dates = sorted(set(r['date'] for r in fk_daily_rows if r.get('date')))
        _me_daily_dates = sorted(set(r['date'] for r in me_daily_rows  if r.get('date')))

        _me_months = [r.get('month', '') for r in db.get('me_monthly', [])]
        _fk_months = [r.get('month', '') for r in db.get('fk_monthly', [])]

        # Streams with genuine daily-granularity tables get gaps computed from
        # their OWN data's date field — same pattern as me_views/me_daily/fk_daily
        # above. Reload ads DBs fresh so this is correct even on a run where the
        # ads-specific save block above didn't execute (no new ads rows today).
        _fk_orders_dates  = sorted(set(r['date'] for r in fk_orders_daily_rows  if r.get('date')))
        _fk_returns_dates = sorted(set(r['date'] for r in fk_returns_daily_rows if r.get('date')))
        try:
            _fk_ads_dates = sorted(set(r['date'] for r in load_fk_ads_db(DB_FK_ADS_PATH).get('fk_ads_daily', []) if r.get('date')))
        except Exception as _e:
            _fk_ads_dates = []
            # Was indistinguishable from "genuinely no FK ads data" downstream
            # in stream_gaps/stream_status -- fixed active.md item #70,
            # 2026-07-20, so a read failure is now visibly different from a
            # real data gap.
            _run_warnings.append({'file': 'fk_ads_db', 'type': 'FK', 'reason': f"could not read fk_ads DB for gap detection: {_e}",
                                   'impact': "FK Ads gap-detection couldn't run this run — a real ads data gap may look identical to a read failure in the Data Pipeline Map until this is fixed"})
        try:
            _me_ads_dates = sorted(set(r['date'] for r in load_me_ads_db(DB_ME_ADS_PATH).get('me_ads_daily', []) if r.get('date')))
        except Exception as _e:
            _me_ads_dates = []
            _run_warnings.append({'file': 'me_ads_db', 'type': 'ME', 'reason': f"could not read me_ads DB for gap detection: {_e}",
                                   'impact': "ME Ads gap-detection couldn't run this run — a real ads data gap may look identical to a read failure in the Data Pipeline Map until this is fixed"})

        # ── Pipeline dates log (tracks which streams ran on each date) ─────────
        _dates_log_path = BASE_DIR / 'pipeline_dates_log.json'
        try:
            with open(_dates_log_path, encoding='utf-8') as _dlf:
                _dates_log = _json_rl.load(_dlf)
        except (FileNotFoundError, _json_rl.JSONDecodeError):
            _dates_log = {}

        _type_to_stream = {
            'ME_ORDERS': 'me_orders',   'ME_RETURNS':  'me_returns',
            'ME_PAYMENTS': 'me_payments', 'ME_ADS':    'me_ads',
            'FK_PAYMENTS': 'fk_payments', 'FK_ADS':    'fk_ads',
            'FK_ADS_CAMPAIGN': 'fk_ads',  'FK_VIEWS':  'fk_views',
            'FK_KEYWORDS': 'fk_keywords', 'ME_VIEWS':  'me_views',
            'ME_CLAIMS': 'me_claims',     'FK_CLAIMS': 'fk_claims',
            'FK_LISTINGS': 'fk_listings', 'ME_CATALOG': 'me_catalog',
            'ME_ADS_SUMMARY': 'me_ads',   'FK_RETURNS': 'fk_returns',
            'FK_ORDERS': 'fk_orders',
            'FK_ADS_DAILY': 'fk_ads',     'FK_ADS_FSN': 'fk_ads',
            'FK_ADS_KW': 'fk_ads',        'FK_ADS_PLACEMENTS': 'fk_ads',
            'FK_ADS_OVERALL': 'fk_ads',   'FK_ADS_SEARCH': 'fk_ads',
            'FK_ADS_ORDERS': 'fk_ads',
        }
        for _fp in processed_files:
            _sid = _type_to_stream.get(typed.get(_fp, ''))
            if _sid:
                _existing_dates = set(_dates_log.get(_sid, []))
                _existing_dates.add(TODAY)
                _dates_log[_sid] = sorted(_existing_dates)

        with open(_dates_log_path, 'w', encoding='utf-8') as _dlf:
            _json_rl.dump(_dates_log, _dlf, indent=2)

        # ── Build stream_gaps ─────────────────────────────────────────────────
        # Each stream's gaps are computed only from that stream's OWN data date
        # range. Previously, streams with no explicit entry here fell back to
        # _dates_log (days the pipeline happened to process a file of that
        # TYPE) — but that reflects when the pipeline ran, not what date range
        # the stream's actual data covers, and since one run processes many
        # file types together, nearly every stream ended up with the same
        # fabricated gap list regardless of its own real data cutoff.
        # Streams with no genuine daily-cadence data (claims, keywords,
        # listings — naturally sparse, not expected every day) intentionally
        # get no stream_gaps entry rather than a fabricated one.
        _az_orders_dates  = sorted(set(r['order_date']  for r in az_orders_daily_rows  if r.get('order_date')))
        _az_returns_dates = sorted(set(r['return_date'] for r in az_returns_daily_rows if r.get('return_date')))

        _stream_gaps = {
            'me_views':    _find_gaps(_me_views_dates),
            'me_orders':   _find_gaps(_me_daily_dates),
            'me_returns':  _find_gaps(_me_daily_dates),
            'fk_views':    _find_gaps(_fk_daily_dates),
            'me_monthly':  _find_month_gaps(_me_months),
            'fk_monthly':  _find_month_gaps(_fk_months),
            'fk_orders':   _find_gaps(_fk_orders_dates),
            'fk_returns':  _find_gaps(_fk_returns_dates),
            'fk_ads':      _find_gaps(_fk_ads_dates),
            'me_ads':      _find_gaps(_me_ads_dates),
            'az_orders':   _find_gaps(_az_orders_dates),
            'az_returns':  _find_gaps(_az_returns_dates),
        }

        # ── Manifest cross-check (Auto-Sync claims vs. what we actually got) ──
        # Answers: is Auto-Sync's per-file "Verified" claim (download_manifest
        # .csv) backed up by real ingested data in THIS pipeline? See
        # rumee-auto-sync DOCS.md Section 25 for the manifest's own spec/limits.
        try:
            from drive_connector import fetch_download_manifest
            _manifest_rows = fetch_download_manifest()
        except Exception as _e:
            print(f"  Manifest cross-check: unavailable this run ({_e})")
            _manifest_rows = []
            _run_warnings.append({'file': 'auto_sync_manifest', 'type': 'INFRA', 'reason': f"Auto-Sync manifest fetch failed: {_e}",
                                   'impact': "cosmetic only — the Data Pipeline Map's file-count cross-check is unavailable this run, no business data affected"})

        # me_views is the one exception (append-type source, no per-day
        # processed_file key — see _STREAM_FILE_PREFIXES) and uses its own
        # per-row Date column directly; everything else is derived from
        # processed_file: config keys via _dated_processed_files.
        _manifest_daily_dates = {'me_views': set(_me_views_dates)}
        for _sid, _prefixes in _STREAM_FILE_PREFIXES.items():
            _manifest_daily_dates[_sid] = _dated_processed_files(db, *_prefixes)

        _manifest_cross_check = _build_manifest_cross_check(
            _manifest_rows, _manifest_daily_dates, TODAY
        )
        if _manifest_cross_check is None:
            print("  Manifest cross-check: unavailable this run (no manifest rows)")
        else:
            print(f"  Manifest cross-check: {_manifest_cross_check['total_discrepancies']} discrepancy(ies) "
                  f"across last {MANIFEST_CROSS_CHECK_WINDOW_DAYS} days")

        # ── Wishlist check (before run log so count goes into log) ────────────
        _prev_wishlist_count = 0
        try:
            _existing_rl = BASE_DIR / 'pipeline_run_log.json'
            if _existing_rl.exists():
                _prev_wishlist_count = _json_rl.loads(_existing_rl.read_text(encoding='utf-8')).get('wishlist_pending_count', 0)
        except Exception as _e:
            _run_warnings.append({'file': 'pipeline_run_log.json', 'type': 'INFRA', 'reason': f"could not read previous wishlist count: {_e}",
                                   'impact': "cosmetic only — a new-wishlist-item Discord alert might fire again for already-seen items this run"})
        _wishlist_pending = []
        try:
            _wl_path = BASE_DIR / 'vantage_wishlist.json'
            if _wl_path.exists():
                _wishlist_pending = [w for w in _json_rl.loads(_wl_path.read_text(encoding='utf-8')) if w.get('status') == 'pending']
        except Exception as _e:
            _run_warnings.append({'file': 'vantage_wishlist.json', 'type': 'INFRA', 'reason': f"could not read vantage_wishlist.json: {_e}",
                                   'impact': "cosmetic only — the Vantage wishlist badge/notification won't reflect pending items this run"})

        # ── Live pipeline status from actual DB row counts ────────────────────
        # Count rows per table across all DB files (read fresh — after all saves
        # above). Lets the dashboard show the REAL Processed/Partial/Not-processed
        # status each run instead of the hardcoded manifest values. Streams not in
        # _STREAM_TABLES (me_catalog, no-source) fall back to manifest on the UI.
        _tbl_counts = {_t: len(_rows) for _t, _rows in db.items() if _t != 'config'}
        for _dbp in (DB_DAILY_PATH, DB_ME_ADS_PATH, DB_FK_ADS_PATH, DB_KEYWORDS_PATH):
            try:
                for _t, _rows in load_db(_dbp).items():
                    _tbl_counts[_t] = _tbl_counts.get(_t, 0) + len(_rows)
            except Exception:
                pass

        _stream_status = {}
        _stream_rows   = {}
        for _sid, _tbls in _STREAM_TABLES.items():
            if not _tbls:
                continue
            _counts = {_t: _tbl_counts.get(_t, 0) for _t in _tbls}
            _stream_rows[_sid] = _counts
            _nonzero = sum(1 for _v in _counts.values() if _v > 0)
            _stream_status[_sid] = ('ok'   if _nonzero == len(_counts)
                                    else 'gap' if _nonzero == 0
                                    else 'partial')

        # ── Build run log ─────────────────────────────────────────────────────
        _sentinel = '1970-01-01'
        def _cfg(key):
            _v = get_config(db, key)
            return None if (not _v or _v == _sentinel) else _v

        _run_status = ('failed'  if _run_errors
                       else 'warning' if _run_warnings
                       else 'ok')
        _run_log = {
            'last_run': datetime.now().isoformat()[:19],
            'run_status': _run_status,
            'errors':   _run_errors,
            'warnings': _run_warnings,
            'stream_dates': {
                'me_orders':   _cfg('me_orders_last_date'),
                'me_returns':  _cfg('me_returns_last_date'),
                'me_payments': _cfg('me_payments_last_date'),
                'me_ads':      _cfg('me_ads_last_date'),
                'me_views':    _me_views_last,
                'me_claims':   _cfg('me_claims_last_date'),
                'me_catalog':  _cfg('me_catalog_last_date'),
                'fk_payments': _cfg('fk_payments_last_date'),
                'fk_ads':      max(_fk_ads_dates) if _fk_ads_dates else None,
                'fk_views':    _cfg('fk_views_last_date'),
                'fk_keywords': _cfg('fk_keywords_last_date'),
                'fk_claims':   _cfg('fk_claims_last_date'),
                'fk_listings': _cfg('fk_listings_last_date'),
                'fk_orders':   fk_orders_last if fk_orders_last != '2026-01-01' else None,
                'fk_returns':  _cfg('fk_payments_last_date'),
                'az_monthly':    (max((r['month'] for r in db.get('az_monthly', [])), default=None)),
                'az_orders':     _cfg('az_orders_last_date'),
                'az_returns':    _cfg('az_returns_last_date'),
                'az_settlement': _cfg('az_settlement_last_created'),
            },
            'stream_gaps': _stream_gaps,
            'stream_status': _stream_status,
            'stream_rows': _stream_rows,
            'manifest_cross_check': _manifest_cross_check,
            'wishlist_pending_count': len(_wishlist_pending),
        }
        with open(BASE_DIR / 'pipeline_run_log.json', 'w', encoding='utf-8') as _rl:
            _json_rl.dump(_run_log, _rl, indent=2)
        print(f"  pipeline_run_log.json updated")
    except Exception as _e:
        import traceback as _rl_tb
        print(f"  Warning: could not write pipeline_run_log.json — {_e}")
        _rl_tb.print_exc()
        # The run log write itself failing had zero downstream alert -- fixed
        # active.md item #70, 2026-07-20 (still attempts the Firestore
        # notification sync below regardless, since that's independent of
        # this local file write).
        _run_warnings.append({'file': 'pipeline_run_log.json', 'type': 'INFRA', 'reason': f"could not write pipeline_run_log.json: {_e}",
                               'impact': "the Data Pipeline Map may show a stale run log until a future run succeeds in writing it"})

    # Notification Center sync (active.md item #70, 2026-07-20) -- pushes
    # every error/warning gathered this run (and auto-resolves ones from
    # categories that are clean again) to Firestore so they surface in the
    # dashboard's bell icon, not just this local JSON file. Best-effort: a
    # failure here must not block Discord/the rest of the run.
    try:
        from firestore_connector import sync_pipeline_notifications
        sync_pipeline_notifications(_run_errors, _run_warnings, datetime.now().isoformat()[:19])
    except Exception as _notif_e:
        print(f"  Warning: could not sync pipeline notifications to Firestore — {_notif_e}")

    send_discord_notification(
        files_processed=len(processed_files),
        files_detail=[fp.name for fp in processed_files],
        summary_rows=summary_rows,
        daily_rows=daily_rows,
        kw_rows_count=len(kw_rows),
        daily_range=daily_range,
        me_orders_last=me_orders_last,
        fk_views_last=fk_views_last,
        fk_orders_last=fk_orders_last,
        stock_summary=_stock_summary,
        return_summary=_return_summary,
    )
    if _wishlist_pending and len(_wishlist_pending) > _prev_wishlist_count:
        send_discord_wishlist_notification(_wishlist_pending[_prev_wishlist_count:])

# ─── Insights Generator ───────────────────────────────────────────────────────

def generate_insights(db):
    """
    Check latest SKU data against thresholds and write new insights to Firestore.
    Called after DB save on every successful pipeline run.
    Firestore failures are always swallowed — this never crashes the pipeline.
    """
    try:
        from firestore_connector import write_insight, write_task, insight_exists_today
    except ImportError:
        print("firestore_connector not available — skipping insight generation")
        return

    if not os.environ.get('FIREBASE_CREDENTIALS'):
        print("FIREBASE_CREDENTIALS not set — skipping insight generation")
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

    print(f"Insights generated: {insights_written} new insights written to Firestore")


# ─── Task Completion Review ───────────────────────────────────────────────────

def review_completed_tasks(db):
    """
    Check recently completed tasks against latest data.
    If the underlying issue persists despite being marked done — reopen it.
    If the issue is genuinely resolved — mark the linked insight as resolved.
    """
    try:
        try:
            from firestore_connector import (write_insight, write_task,
                                              mark_insight_resolved,
                                              get_completed_tasks_with_insights)
        except ImportError:
            return

        if not os.environ.get('FIREBASE_CREDENTIALS'):
            return

        from datetime import timedelta, timezone as _tz
        cutoff = (datetime.now(_tz.utc) - timedelta(days=7)).isoformat()

        completed_tasks = get_completed_tasks_with_insights(cutoff)
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
        _run_warnings.append({'file': 'review_completed_tasks', 'type': 'INFRA', 'reason': f"review_completed_tasks failed: {e}",
                               'impact': "resolved/reopened Tasks-tab items weren't re-checked this run — status may be stale until a future run succeeds"})


# ─── Discord Notification ─────────────────────────────────────────────────────

def send_discord_notification(files_processed, files_detail, summary_rows,
                              daily_rows, kw_rows_count, daily_range,
                              me_orders_last, fk_views_last, fk_orders_last=None,
                              stock_summary=None, return_summary=None):
    """Post a pipeline-run summary embed to the Rumee Discord server."""
    import urllib.request
    import urllib.error

    WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL_PIPELINE')
    if not WEBHOOK_URL:
        try:
            from rumee_secrets import DISCORD_WEBHOOK_URL
            WEBHOOK_URL = DISCORD_WEBHOOK_URL
        except ImportError:
            print("Discord webhook not configured — skipping notification")
            return

    files_list = '\n'.join(f'• {f}' for f in files_detail) or '(none)'
    embed = {
        'title': f'\U0001f4ca Rumee Pipeline — {TODAY}',
        'color': 0x27ae60,
        'fields': [
            {'name': 'Files processed', 'value': f'{files_processed} file(s)\n{files_list}', 'inline': False},
            {'name': 'DB rows', 'value': f'Summary: {summary_rows}  |  Daily: {daily_rows}  |  Keywords: {kw_rows_count}', 'inline': False},
            {'name': 'Data window', 'value': daily_range or 'N/A', 'inline': True},
            {'name': 'ME orders up to', 'value': me_orders_last, 'inline': True},
            {'name': 'FK views up to', 'value': fk_views_last, 'inline': True},
            {'name': 'FK orders up to', 'value': fk_orders_last or 'N/A', 'inline': True},
        ],
    }
    # Sale-triggered stock decrement + return credit-back visibility (item #64,
    # 2026-07-17 -- Golden Rule 29: these counts previously only reached a
    # console print in CI logs, silently invisible day to day. "No BOM yet" is
    # expected/ongoing while Jaiswal builds BOMs one at a time; watching it
    # here is what makes an unexpected spike or an orphaned-BOM regression
    # (see DOCS.md §27 invariant #11) noticeable instead of silent.
    ss = stock_summary or {}
    rs = return_summary or {}
    embed['fields'].append({
        'name': 'Stock (sale/return)',
        'value': (f"Resolved: {ss.get('resolved', 0)}  |  Movements: {ss.get('movements', 0)}  |  "
                  f"No BOM yet: {ss.get('no_bom', 0)}  |  Unresolved SKU: {ss.get('unresolved', 0)}  |  "
                  f"Return credits: {rs.get('orders_credited', 0)}"),
        'inline': False,
    })
    payload = json.dumps({'embeds': [embed]}).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'RumeePipeline/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Discord notification sent (HTTP {resp.status})")
    except urllib.error.URLError as e:
        print(f"Discord notification failed: {e}")


def send_discord_pm_overrides_alert(error_msg):
    """Post an alert when pm_overrides fails to load — CATALOG/FK_LISTINGS/Amazon
    product_master processing is skipped for the run when this fires (see the
    pm_overrides_load_failed gate in main()), so this needs to be loud rather
    than a buried log line."""
    import urllib.request
    import urllib.error

    WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL_PIPELINE')
    if not WEBHOOK_URL:
        try:
            from rumee_secrets import DISCORD_WEBHOOK_URL
            WEBHOOK_URL = DISCORD_WEBHOOK_URL
        except ImportError:
            print("Discord webhook not configured — skipping pm_overrides failure alert")
            return

    embed = {
        'title': f'⚠️ Rumee Pipeline — pm_overrides load FAILED ({TODAY})',
        'color': 0xe74c3c,
        'description': (
            'CATALOG, FK_LISTINGS, and Amazon product_master processing were '
            'SKIPPED this run — the affected files were left unprocessed and '
            'will retry automatically next run. No needs_review rows were '
            'created from this failure.'
        ),
        'fields': [{'name': 'Error', 'value': str(error_msg)[:1000], 'inline': False}],
    }
    payload = json.dumps({'embeds': [embed]}).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'RumeePipeline/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Discord pm_overrides-failure alert sent (HTTP {resp.status})")
    except urllib.error.URLError as e:
        print(f"Discord pm_overrides-failure alert failed to send: {e}")


def send_discord_wishlist_notification(new_items):
    """Post a Vantage wishlist update embed when new pending items are added."""
    import urllib.request
    import urllib.error

    WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL_PIPELINE')
    if not WEBHOOK_URL:
        try:
            from rumee_secrets import DISCORD_WEBHOOK_URL
            WEBHOOK_URL = DISCORD_WEBHOOK_URL
        except ImportError:
            print("Discord webhook not configured — skipping notification")
            return

    lines = '\n'.join(
        f"• [{item.get('priority','?').upper()}] {item.get('data_needed', item.get('id'))}"
        for item in new_items
    )
    embed = {
        'title': '\U0001f9e0 Vantage data request',
        'description': f'Vantage needs {len(new_items)} new data stream(s):',
        'color': 0xe67e22,
        'fields': [
            {'name': 'Pending items', 'value': lines, 'inline': False},
        ],
    }
    payload = json.dumps({'embeds': [embed]}).encode('utf-8')
    req = urllib.request.Request(
        WEBHOOK_URL,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'RumeePipeline/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"Discord wishlist notification sent (HTTP {resp.status})")
    except urllib.error.URLError as e:
        print(f"Discord wishlist notification failed: {e}")


if __name__ == '__main__':
    import traceback as _tb_main
    try:
        main()
    except Exception as _crash:
        print(f"\n  PIPELINE CRASH: {_crash}")
        _tb_main.print_exc()
        # Write a CRASH entry to the log even though flush_log wasn't called
        with open(LOG_PATH, 'a', encoding='utf-8') as _lf:
            _lf.write(f"[CRASH] {TODAY} — {_crash}\n")
            _lf.write(_tb_main.format_exc())
        # Best-effort, separately-wrapped push of a single critical
        # notification -- today a full crash left the dashboard showing a
        # stale log with zero indication anything failed. active.md item
        # #70, 2026-07-20.
        try:
            from firestore_connector import sync_pipeline_notifications
            _crash_entry = [{'file': 'pipeline_crash', 'type': 'INFRA',
                              'reason': f"pipeline crashed: {_crash}\n{_tb_main.format_exc()[-1500:]}",
                              'impact': "today's pipeline run did not complete — data may be stale, check which streams actually finished"}]
            sync_pipeline_notifications(_crash_entry, [], datetime.now().isoformat()[:19])
        except Exception as _notif_crash_e:
            print(f"  Warning: could not push crash notification — {_notif_crash_e}")
        raise
