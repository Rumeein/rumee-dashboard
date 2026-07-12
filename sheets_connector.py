"""
Rumee Dashboard — Google Sheets Connector (Orders Ledger)

Creates and manages the rumee_orders_ledger Google Sheet.
Sheet is created once; ID stored in pipeline config as 'ledger_sheet_id'.
Subsequent runs reuse the same sheet.

Auth: same service-account credentials as drive_connector.py
  1. GOOGLE_DRIVE_CREDENTIALS env var (JSON string)  <- GitHub Actions
  2. credentials.json in project root                <- local use

Scopes needed (broader than drive_connector which is readonly):
  - spreadsheets  : create / read / write sheets
  - drive         : share sheet with owner on first creation
"""

import os
import json
from pathlib import Path
from datetime import datetime, timedelta

LEDGER_TITLE   = 'Rumee Orders Ledger'
ORDERS_TAB     = 'orders'
OWNER_EMAIL    = 'rumeein@gmail.com'

FINAL_STATUSES = {'Delivered', 'Returned-Customer', 'RTO', 'Cancelled'}

LEDGER_COLUMNS = [
    'order_id', 'order_date', 'platform', 'sku', 'qty',
    'gmv', 'settlement',
    'commission', 'fixed_fee', 'collection_fee',
    'shipping_fwd', 'shipping_rev',
    'gst_on_fees', 'tcs', 'tds', 'penalty',
    'cogs', 'packaging_cost', 'ad_spend_apport',
    'status', 'zone', 'is_shopsy',
    'return_reason', 'earring_condition', 'box_condition', 'chain_condition',
    'return_loss_value', 'packaging_loss', 'chain_loss',
    'claim_id', 'claim_status', 'claim_recovered',
    'net_pl',
]

_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _build_service(api, version):
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise ImportError(
            "Google API client not installed.\n"
            "Run: pip install google-api-python-client google-auth"
        )
    creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    if creds_json:
        creds_info = json.loads(creds_json)
    else:
        creds_path = Path(__file__).parent / 'credentials.json'
        if not creds_path.exists():
            raise FileNotFoundError(
                "No credentials found. Set GOOGLE_DRIVE_CREDENTIALS or place credentials.json in project root."
            )
        creds_info = json.loads(creds_path.read_text(encoding='utf-8'))

    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=_SCOPES)
    return build(api, version, credentials=creds, cache_discovery=False)


# ─── Sheet lifecycle ──────────────────────────────────────────────────────────

def get_or_create_ledger(config_get, config_set):
    """
    Returns sheet_id of the Orders Ledger.
    Creates the sheet on first call, stores ID via config_set().
    config_get / config_set are callables matching process.py's get_config / set_config.
    """
    sheet_id = config_get('ledger_sheet_id')
    if sheet_id and sheet_id not in ('', 'None'):
        return sheet_id

    print("  [Ledger] No sheet ID found — creating new Orders Ledger sheet...")
    svc = _build_service('sheets', 'v4')

    body = {
        'properties': {'title': LEDGER_TITLE},
        'sheets': [{'properties': {'title': ORDERS_TAB}}],
    }
    resp      = svc.spreadsheets().create(body=body, fields='spreadsheetId').execute()
    sheet_id  = resp['spreadsheetId']
    print(f"  [Ledger] Created sheet: {sheet_id}")

    # Write header row
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f'{ORDERS_TAB}!A1',
        valueInputOption='RAW',
        body={'values': [LEDGER_COLUMNS]},
    ).execute()

    # Share with owner
    try:
        drive_svc = _build_service('drive', 'v3')
        drive_svc.permissions().create(
            fileId=sheet_id,
            body={'type': 'user', 'role': 'writer', 'emailAddress': OWNER_EMAIL},
            sendNotificationEmail=False,
        ).execute()
        print(f"  [Ledger] Shared with {OWNER_EMAIL}")
    except Exception as e:
        print(f"  [Ledger] Warning: could not share sheet — {e}")

    config_set('ledger_sheet_id', sheet_id)
    return sheet_id


# ─── Read ─────────────────────────────────────────────────────────────────────

def read_all_rows(sheet_id):
    """
    Returns (header, rows_as_dicts, row_index).
    row_index: {order_id: sheet_row_number (1-based, row 1 = header)}
    """
    svc = _build_service('sheets', 'v4')
    result = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=f'{ORDERS_TAB}!A:AH',
    ).execute()
    values = result.get('values', [])
    if not values:
        return LEDGER_COLUMNS, [], {}

    header = values[0]
    rows   = []
    index  = {}
    for i, row in enumerate(values[1:], start=2):  # row 2 = first data row
        padded = row + [''] * (len(header) - len(row))
        d = dict(zip(header, padded))
        rows.append(d)
        if d.get('order_id'):
            index[d['order_id']] = i
    return header, rows, index


def fetch_open_orders(sheet_id, days=30):
    """
    Returns list of {order_id, sheet_row} for non-final orders within last N days.
    Used by the status-update pass.
    """
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')
    _, rows, index = read_all_rows(sheet_id)
    open_orders = []
    for row in rows:
        if row.get('status') in FINAL_STATUSES:
            continue
        if row.get('order_date', '') < cutoff:
            continue
        oid = row.get('order_id', '')
        if oid and oid in index:
            open_orders.append({'order_id': oid, 'sheet_row': index[oid], **row})
    return open_orders


# ─── Write ────────────────────────────────────────────────────────────────────

def upsert_rows(sheet_id, new_rows):
    """
    Upsert rows into the ledger sheet keyed by order_id.
    - Existing order_id → update that row in place (preserves sheet_row).
    - New order_id → append to end.
    Returns (inserted_count, updated_count).
    """
    if not new_rows:
        return 0, 0

    _, _, index = read_all_rows(sheet_id)
    svc = _build_service('sheets', 'v4')

    to_append  = []
    batch_data = []  # for batchUpdate of existing rows

    for row in new_rows:
        oid = row.get('order_id', '')
        values = [str(row.get(c, '') or '') for c in LEDGER_COLUMNS]

        if oid and oid in index:
            sheet_row = index[oid]
            batch_data.append({
                'range': f'{ORDERS_TAB}!A{sheet_row}',
                'values': [values],
            })
        else:
            to_append.append(values)

    updated = 0
    if batch_data:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={'valueInputOption': 'RAW', 'data': batch_data},
        ).execute()
        updated = len(batch_data)

    inserted = 0
    if to_append:
        svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f'{ORDERS_TAB}!A1',
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body={'values': to_append},
        ).execute()
        inserted = len(to_append)

    return inserted, updated


# ─── Return receipts ──────────────────────────────────────────────────────────

RETURN_RECEIPTS_ID = '1R5JRyFXYu-85426QwhwZpWmL_BrjLd5-BPER1T23zVY'

def fetch_return_receipts():
    """
    Reads return_receipts sheet and returns a dict keyed by order_id AND awb:
      {order_id: {earring_condition, box_condition, chain_condition}, awb: {...}}
    Earring column = 'Earring Intact', box = 'Box Intact', chain = 'Chain
    Intact' (added 2026-07-12, dashboard memory active.md #46 -- rows
    scanned before this column existed will simply have '' here, which the
    ledger-building side treats as chain always lost, matching Jaiswal's
    explicit rule for historical returns). Values: Intact / Damaged.
    """
    svc = _build_service('sheets', 'v4')
    result = svc.spreadsheets().values().get(
        spreadsheetId=RETURN_RECEIPTS_ID,
        range='Receipts!A:Z',
    ).execute()
    values = result.get('values', [])
    if len(values) < 2:
        return {}

    header = [str(h).strip().lower().replace(' ', '_') for h in values[0]]
    receipts = {}

    order_col   = next((i for i, h in enumerate(header) if 'order' in h and 'id' in h), None)
    awb_col     = next((i for i, h in enumerate(header) if 'awb' in h), None)
    earring_col = next((i for i, h in enumerate(header) if 'earring' in h), None)
    box_col     = next((i for i, h in enumerate(header) if 'box' in h), None)
    chain_col   = next((i for i, h in enumerate(header) if 'chain' in h), None)

    for row in values[1:]:
        padded = row + [''] * (len(header) - len(row))
        earring = padded[earring_col].strip() if earring_col is not None else ''
        box     = padded[box_col].strip()     if box_col     is not None else ''
        chain   = padded[chain_col].strip()   if chain_col   is not None else ''
        record  = {'earring_condition': earring, 'box_condition': box, 'chain_condition': chain}

        if order_col is not None:
            oid = padded[order_col].strip()
            if oid:
                receipts[oid] = record

        if awb_col is not None:
            awb = padded[awb_col].strip()
            if awb:
                receipts.setdefault(awb, record)

    return receipts
