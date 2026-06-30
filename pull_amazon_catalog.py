#!/usr/bin/env python3
"""
pull_amazon_catalog.py

Pulls all Amazon listings via SP-API Reports API and saves as CSV.
Optionally uploads to Google Drive AZ_CATALOG folder.

Official docs followed:
  Reports API: https://developer-docs.amazon.com/sp-api/docs/reports-api-v2021-06-30-reference
  Report types: https://developer-docs.amazon.com/sp-api/docs/report-type-values-inventory

Workflow (per official docs):
  1. POST /reports/2021-06-30/reports         → reportId
  2. GET  /reports/2021-06-30/reports/{id}    → poll until processingStatus=DONE → reportDocumentId
  3. GET  /reports/2021-06-30/documents/{id}  → presigned URL + compressionAlgorithm
  4. Download from presigned URL; decompress if GZIP
  5. Strip UTF-8 BOM; parse tab-delimited → save as CSV
  6. Upload to Drive AZ_CATALOG folder if AZ_CATALOG_DRIVE_FOLDER_ID is set

Usage:
  python pull_amazon_catalog.py

Environment variables:
  AMAZON_LWA_CLIENT_ID       — LWA client ID (or from rumee_secrets.py)
  AMAZON_LWA_CLIENT_SECRET   — LWA client secret
  AMAZON_REFRESH_TOKEN       — LWA refresh token
  GOOGLE_DRIVE_CREDENTIALS   — service-account JSON string (or credentials.json in project root)
  AZ_CATALOG_DRIVE_FOLDER_ID — Drive folder ID to upload CSV into (optional)
"""

import csv
import gzip
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL      = 'https://sellingpartnerapi-eu.amazon.com'   # EU endpoint serves India marketplace
MKT_ID        = 'A21TJRUUN4KGV'                             # India marketplace ID
REPORT_TYPE   = 'GET_MERCHANT_LISTINGS_ALL_DATA'             # Full listing data incl. ASIN, title, price, qty
POLL_INTERVAL = 30     # seconds between status checks
MAX_POLLS     = 20     # 10 minutes total before timeout


def _log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"  [{ts}] {msg}")


# ── Credentials ─────────────────────────────────────────────────────────────────
def _get_creds():
    client_id     = os.environ.get('AMAZON_LWA_CLIENT_ID')
    client_secret = os.environ.get('AMAZON_LWA_CLIENT_SECRET')
    refresh_token = os.environ.get('AMAZON_REFRESH_TOKEN')

    if not all([client_id, client_secret, refresh_token]):
        try:
            import rumee_secrets as _sec
            client_id     = client_id     or getattr(_sec, 'AMAZON_LWA_CLIENT_ID',     None)
            client_secret = client_secret or getattr(_sec, 'AMAZON_LWA_CLIENT_SECRET', None)
            refresh_token = refresh_token or getattr(_sec, 'AMAZON_REFRESH_TOKEN',     None)
        except ImportError:
            pass

    missing = [k for k, v in {
        'AMAZON_LWA_CLIENT_ID':     client_id,
        'AMAZON_LWA_CLIENT_SECRET': client_secret,
        'AMAZON_REFRESH_TOKEN':     refresh_token,
    }.items() if not v]
    if missing:
        raise ValueError(f"Missing credentials: {', '.join(missing)}\n"
                         "Set env vars or add to rumee_secrets.py")
    return client_id, client_secret, refresh_token


# ── LWA Token ──────────────────────────────────────────────────────────────────
def _get_access_token(client_id, client_secret, refresh_token):
    _log("Requesting LWA access token ...")
    body = urllib.parse.urlencode({
        'grant_type':    'refresh_token',
        'refresh_token': refresh_token,
        'client_id':     client_id,
        'client_secret': client_secret,
    }).encode('utf-8')
    req = urllib.request.Request(
        'https://api.amazon.com/auth/o2/token',
        data=body,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent':   'RumeePipeline/1.0',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ''
        try: detail = e.read().decode('utf-8', errors='replace')[:300]
        except Exception: pass
        raise RuntimeError(f"LWA token exchange failed: HTTP {e.code} — {detail}")

    token = resp.get('access_token')
    if not token:
        raise ValueError(f"No access_token in response: {list(resp.keys())}")
    _log(f"Token OK (expires_in={resp.get('expires_in')}s)")
    return token


# ── SP-API Request Helper ──────────────────────────────────────────────────────
def _sp_request(method, path, access_token, body=None, params=None):
    url = BASE_URL + path
    if params:
        url += '?' + urllib.parse.urlencode(params)
    data = json.dumps(body).encode('utf-8') if body else None
    headers = {
        'x-amz-access-token': access_token,
        'Content-Type':       'application/json',
        'User-Agent':         'RumeePipeline/1.0',
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ''
        try: detail = e.read().decode('utf-8', errors='replace')[:500]
        except Exception: pass
        raise RuntimeError(f"SP-API {method} {path} → HTTP {e.code}: {detail}")


# ── Reports API: Step 1 ─────────────────────────────────────────────────────────
def _create_report(access_token):
    _log(f"Creating report: {REPORT_TYPE} (marketplace={MKT_ID}) ...")
    resp = _sp_request('POST', '/reports/2021-06-30/reports', access_token, body={
        'reportType':     REPORT_TYPE,
        'marketplaceIds': [MKT_ID],
    })
    report_id = resp.get('reportId')
    if not report_id:
        raise ValueError(f"No reportId in response: {resp}")
    _log(f"Report ID: {report_id}")
    return report_id


# ── Reports API: Step 2 ─────────────────────────────────────────────────────────
def _poll_report(access_token, report_id):
    _log(f"Polling status (up to {MAX_POLLS} checks, {POLL_INTERVAL}s apart) ...")
    for attempt in range(1, MAX_POLLS + 1):
        resp = _sp_request('GET', f'/reports/2021-06-30/reports/{report_id}', access_token)
        status = resp.get('processingStatus', 'UNKNOWN')
        _log(f"  [{attempt}/{MAX_POLLS}] processingStatus = {status}")

        if status == 'DONE':
            doc_id = resp.get('reportDocumentId')
            if not doc_id:
                raise ValueError("processingStatus=DONE but no reportDocumentId in response")
            return doc_id

        if status in ('CANCELLED', 'FATAL'):
            raise RuntimeError(f"Report ended with status={status}")

        if attempt < MAX_POLLS:
            time.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Report {report_id} not DONE after {MAX_POLLS} polls "
        f"({MAX_POLLS * POLL_INTERVAL}s). Try again later."
    )


# ── Reports API: Step 3 ─────────────────────────────────────────────────────────
def _get_document_url(access_token, doc_id):
    _log(f"Getting document download URL ...")
    resp = _sp_request('GET', f'/reports/2021-06-30/documents/{doc_id}', access_token)
    url = resp.get('url')
    compression = resp.get('compressionAlgorithm')
    if not url:
        raise ValueError(f"No url in document response: {resp}")
    _log(f"URL obtained (compression={compression or 'none'})")
    return url, compression


# ── Reports API: Step 4 — Download ─────────────────────────────────────────────
def _download_report(url, compression):
    _log("Downloading report file ...")
    req = urllib.request.Request(url, headers={'User-Agent': 'RumeePipeline/1.0'})
    with urllib.request.urlopen(req, timeout=120) as r:
        raw = r.read()
    _log(f"Downloaded {len(raw):,} bytes (compressed)")

    if compression and compression.upper() == 'GZIP':
        raw = gzip.decompress(raw)
        _log(f"Decompressed to {len(raw):,} bytes")

    # Strip UTF-8 BOM — Amazon reports always include it (per docs)
    if raw.startswith(b'\xef\xbb\xbf'):
        raw = raw[3:]

    return raw.decode('utf-8', errors='replace')


# ── Step 5 — Parse tab-delimited → CSV ────────────────────────────────────────
def _save_csv(text, out_path):
    reader = csv.DictReader(io.StringIO(text), delimiter='\t')
    rows = list(reader)
    if not rows:
        raise ValueError("Report returned 0 rows — no listings found in India marketplace")

    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    _log(f"CSV saved: {out_path}")
    _log(f"  Rows:    {len(rows)}")
    _log(f"  Columns: {', '.join(reader.fieldnames)}")
    return rows, reader.fieldnames


# ── Step 6 — Upload to Google Drive ───────────────────────────────────────────
def _upload_to_drive(csv_path, folder_id):
    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload
        from google.oauth2 import service_account
    except ImportError:
        raise ImportError(
            "google-api-python-client and google-auth are required for Drive upload.\n"
            "  pip install google-api-python-client google-auth"
        )

    creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    creds_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'credentials.json')

    if creds_json:
        creds_info = json.loads(creds_json)
    elif os.path.exists(creds_path):
        with open(creds_path, encoding='utf-8') as f:
            creds_info = json.load(f)
    else:
        raise FileNotFoundError(
            "No Drive credentials found.\n"
            "  Option A: set GOOGLE_DRIVE_CREDENTIALS env var to service-account JSON string\n"
            "  Option B: place credentials.json in the project root"
        )

    # 'drive' scope required — 'drive.file' only works for files created by the app itself
    scopes = ['https://www.googleapis.com/auth/drive']
    creds   = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    filename = os.path.basename(csv_path)
    query    = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = service.files().list(q=query, fields='files(id,name)').execute().get('files', [])

    media = MediaFileUpload(csv_path, mimetype='text/csv', resumable=False)

    if existing:
        file_id = existing[0]['id']
        service.files().update(fileId=file_id, media_body=media).execute()
        _log(f"Drive: updated existing file '{filename}' (id={file_id})")
    else:
        meta   = {'name': filename, 'parents': [folder_id]}
        result = service.files().create(body=meta, media_body=media, fields='id').execute()
        _log(f"Drive: created new file '{filename}' (id={result.get('id')})")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    today    = date.today().isoformat()
    out_name = f"az_catalog_{today}.csv"
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), out_name)

    drive_folder_id = os.environ.get('AZ_CATALOG_DRIVE_FOLDER_ID')
    if not drive_folder_id:
        try:
            import rumee_secrets as _sec
            drive_folder_id = getattr(_sec, 'AZ_CATALOG_DRIVE_FOLDER_ID', None)
        except ImportError:
            pass

    print(f"\n{'='*60}")
    print(f"  Amazon Catalog Pull — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    try:
        # Step 0: credentials
        client_id, client_secret, refresh_token = _get_creds()

        # Step 1: LWA token
        access_token = _get_access_token(client_id, client_secret, refresh_token)

        # Step 2: create report
        report_id = _create_report(access_token)

        # Step 3: poll until DONE
        doc_id = _poll_report(access_token, report_id)

        # Step 4: get presigned URL
        url, compression = _get_document_url(access_token, doc_id)

        # Step 5: download + decompress
        text = _download_report(url, compression)

        # Step 6: parse + save CSV
        rows, fields = _save_csv(text, out_path)

        # Step 7: Drive upload
        if drive_folder_id:
            _log(f"\nUploading to Drive folder: {drive_folder_id}")
            _upload_to_drive(out_path, drive_folder_id)
            _log("Drive upload complete.")
        else:
            _log(
                "\nDrive upload skipped.\n"
                "  To enable: set AZ_CATALOG_DRIVE_FOLDER_ID env var to the Drive folder ID.\n"
                "  Also ensure the service account has Editor access on that folder."
            )

        print(f"\n{'='*60}")
        print(f"  DONE — {len(rows)} listings saved to {out_name}")
        print(f"{'='*60}\n")

    except (ValueError, RuntimeError, TimeoutError, FileNotFoundError) as e:
        print(f"\n  ERROR: {e}\n", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n  Interrupted.", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
