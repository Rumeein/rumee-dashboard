"""
Rumee Dashboard — Amazon SP-API Connector (Reports API)

Handles the Orders/Settlement/Returns bulk report acquisition for the
Orders Ledger (dashboard memory active.md item #57, 2026-07-14). Mirrors
drive_connector.py / sheets_connector.py's role: a thin, dedicated wrapper
around one external API, called from process.py's main().

Report types (confirmed against live Amazon SP-API docs, 2026-07-14 --
NOT the deprecated GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE, which stopped
working 2026-03-25):
  REPORT_TYPE_ORDERS     = GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL
  REPORT_TYPE_SETTLEMENT = GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2
  REPORT_TYPE_RETURNS    = GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE

Auth: standard LWA refresh-token flow
  1. AMAZON_LWA_CLIENT_ID / AMAZON_LWA_CLIENT_SECRET / AMAZON_REFRESH_TOKEN
     env vars <- GitHub Actions
  2. rumee_secrets.py <- local use

Rate limits (confirmed 2026-07-14): createReport 0.0167 req/sec burst 15,
getReports 0.0222 req/sec burst 10 -- generous for a once-daily cadence,
no throttling logic needed beyond the small fixed sleeps already used
elsewhere in this codebase for Amazon calls.

Report completion is NOT synchronous or guaranteed within one pipeline
run -- callers must treat request/poll/download as STATEFUL ACROSS RUNS
(store the pending reportId in db config, check status on a later run),
the same pattern process.py already uses for Drive-file watermarks.
"""

import os
import io
import gzip
import json
import time
import urllib.request
import urllib.error
import urllib.parse

BASE_URL = 'https://sellingpartnerapi-eu.amazon.com'
MKT_ID   = 'A21TJRUUN4KGV'   # India marketplace

REPORT_TYPE_ORDERS     = 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL'
REPORT_TYPE_SETTLEMENT = 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2'
REPORT_TYPE_RETURNS    = 'GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE'

# Terminal processingStatus values -- IN_QUEUE/IN_PROGRESS mean "keep waiting"
TERMINAL_STATUSES = {'DONE', 'CANCELLED', 'FATAL'}


class AmazonApiError(Exception):
    """Raised for any SP-API HTTP/auth failure -- callers append to _run_errors/_run_warnings (Golden Rule 29, no silent errors)."""
    pass


def _get_credentials():
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
        'AMAZON_LWA_CLIENT_ID': client_id,
        'AMAZON_LWA_CLIENT_SECRET': client_secret,
        'AMAZON_REFRESH_TOKEN': refresh_token,
    }.items() if not v]
    if missing:
        raise AmazonApiError(f"Credentials missing: {', '.join(missing)}")
    return client_id, client_secret, refresh_token


def _get_access_token():
    """LWA token exchange -- POST https://api.amazon.com/auth/o2/token."""
    client_id, client_secret, refresh_token = _get_credentials()
    body = urllib.parse.urlencode({
        'grant_type':    'refresh_token',
        'refresh_token': refresh_token,
        'client_id':     client_id,
        'client_secret': client_secret,
    }).encode('utf-8')
    req = urllib.request.Request(
        'https://api.amazon.com/auth/o2/token',
        data=body,
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'User-Agent': 'RumeePipeline/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ''
        try: detail = e.read().decode('utf-8', errors='replace')[:300]
        except Exception: pass
        raise AmazonApiError(f"LWA token exchange failed: HTTP {e.code} {e.reason} — {detail}")
    except Exception as e:
        raise AmazonApiError(f"LWA token exchange failed: {e}")
    access_token = resp.get('access_token')
    if not access_token:
        raise AmazonApiError(f"No access_token in LWA response: {list(resp.keys())}")
    return access_token


def _sp_request(method, path, access_token, params=None, body=None):
    query = ('?' + urllib.parse.urlencode(params)) if params else ''
    req = urllib.request.Request(
        f"{BASE_URL}{path}{query}",
        data=json.dumps(body).encode('utf-8') if body is not None else None,
        headers={
            'x-amz-access-token': access_token,
            'Content-Type':       'application/json',
            'User-Agent':         'RumeePipeline/1.0',
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        detail = ''
        try: detail = e.read().decode('utf-8', errors='replace')[:400]
        except Exception: pass
        raise AmazonApiError(f"HTTP {e.code} {e.reason} on {method} {path} — {detail}")
    except Exception as e:
        raise AmazonApiError(f"Exception on {method} {path}: {e}")


def create_report(report_type, data_start_time=None, data_end_time=None, marketplace_id=MKT_ID):
    """
    POST /reports/2021-06-30/reports — requests a new report.
    Returns the new reportId (str). Settlement reports CANNOT be created
    on demand (Amazon auto-schedules them) -- use list_reports() instead.
    """
    access_token = _get_access_token()
    body = {'reportType': report_type, 'marketplaceIds': [marketplace_id]}
    if data_start_time:
        body['dataStartTime'] = data_start_time
    if data_end_time:
        body['dataEndTime'] = data_end_time
    resp = _sp_request('POST', '/reports/2021-06-30/reports', access_token, body=body)
    report_id = resp.get('reportId')
    if not report_id:
        raise AmazonApiError(f"createReport response missing reportId: {resp}")
    return report_id


def get_report(report_id):
    """GET /reports/2021-06-30/reports/{reportId} — poll for status."""
    access_token = _get_access_token()
    return _sp_request('GET', f'/reports/2021-06-30/reports/{report_id}', access_token)


def list_reports(report_types, created_since=None, marketplace_id=MKT_ID):
    """
    GET /reports/2021-06-30/reports — used for settlement reports (auto-
    scheduled by Amazon, discoverable only, never created on demand).
    Returns the list of report dicts from the response's 'reports' key.
    """
    access_token = _get_access_token()
    params = {'reportTypes': ','.join(report_types), 'marketplaceIds': marketplace_id}
    if created_since:
        params['createdSince'] = created_since
    resp = _sp_request('GET', '/reports/2021-06-30/reports', access_token, params=params)
    return resp.get('reports', [])


def get_report_document(report_document_id):
    """
    GET /reports/2021-06-30/documents/{reportDocumentId}, then downloads
    the actual file from the returned (5-min-expiring) url. Decompresses
    GZIP automatically if compressionAlgorithm says so. Returns the
    decoded text content.
    """
    access_token = _get_access_token()
    meta = _sp_request('GET', f'/reports/2021-06-30/documents/{report_document_id}', access_token)
    url = meta.get('url')
    if not url:
        raise AmazonApiError(f"getReportDocument response missing url: {meta}")

    try:
        with urllib.request.urlopen(url, timeout=60) as r:
            raw = r.read()
    except Exception as e:
        raise AmazonApiError(f"Failed downloading report document: {e}")

    if meta.get('compressionAlgorithm') == 'GZIP':
        try:
            raw = gzip.decompress(raw)
        except Exception as e:
            raise AmazonApiError(f"Failed decompressing GZIP report document: {e}")

    encoding = meta.get('reportDocumentEncoding') or 'utf-8'
    try:
        return raw.decode(encoding, errors='replace')
    except LookupError:
        return raw.decode('utf-8', errors='replace')


def poll_until_done(report_id, max_attempts=5, sleep_seconds=30):
    """
    Convenience helper for interactive/manual use. NOT used by the daily
    pipeline (which is stateful-across-runs, see module docstring) — a
    scheduled run should call get_report() once, check processingStatus,
    and persist reportId in config if not yet terminal rather than block
    here for however long Amazon takes.
    """
    for _ in range(max_attempts):
        info = get_report(report_id)
        status = info.get('processingStatus')
        if status in TERMINAL_STATUSES:
            return info
        time.sleep(sleep_seconds)
    return get_report(report_id)
