"""
Rumee Dashboard — Google Drive Connector
Scans configured Drive folders for new export files and downloads them
to a temp directory for processing.

Auth: Service account credentials loaded from:
  1. GOOGLE_DRIVE_CREDENTIALS env var (JSON string)  ← for GitHub Actions
  2. credentials.json in project root               ← for local use

Usage (from process.py):
    from drive_connector import fetch_new_files
    files = fetch_new_files(db)          # -> [(Path, file_type_hint), ...]
    # After successful processing:
    #   set_config(db, f'processed_file:{fp.name}', TODAY)

Install dependencies:
    pip install google-api-python-client google-auth
"""

import os
import io
import json
import tempfile
from pathlib import Path
from datetime import date

TODAY = date.today().isoformat()

# ─── Drive Folder ID → File Type Hint ────────────────────────────────────────
# These map each Google Drive folder to its expected file type so that even if
# auto-detection (sniff_csv_header / sniff_xlsx_header) is uncertain, we know
# what kind of data it contains.
DRIVE_FOLDERS = {
    # Meesho
    '1V0ZnC6r577zYJIYeyDhl8rItBrAXgnwQ': 'ME_ORDERS',        # meesho/orders
    '1MEW8yK9lsercJ5k1gQIRh_xiOHpneSV8': 'ME_RETURNS',       # meesho/returns
    '1e7qdkFu6trp3BQDQdAY22i_INGvzKNeu': 'CATALOG',          # meesho/catalog
    '1DoZoUTmNf6hMqC0-WlS2IWPzTDwyAwQr': 'ME_PAYMENTS',      # meesho/payments
    '1HMThJGvTIVygdjKh1pTyzbEblro4_0sk': 'ME_ADS',           # meesho/ads (parent — no files uploaded here)
    '1yQFg3HuOwtFpEFtx0ZYtBQPChSlpvL54': 'ME_ADS_MASTER',    # meesho/ads/master — lifetime campaign rows
    '18qeRzJmTl6detS6Q3GEuK9gAnn8MZjDB': 'ME_ADS_SUMMARY',   # meesho/ads/summary — daily per-campaign
    '1VDrfDM5uy2Xs2E9XCR7Ijk1BBwh2pO2F': 'ME_ADS_CATALOG',   # meesho/ads/catalog — daily catalog detail
    '1EMqTpDtsratSY66UbbrV4VsnGIXYKFqV': 'ME_VIEWS',         # meesho/views
    '1LX79E16fhxEF5kZGWmdXl4oNsVvqwspf': 'ME_CLAIMS',        # meesho/claims
    # Flipkart
    '1-LzJJo3Wi3x6YrUjYCm7SYm3x2tWQqko': 'FK_ORDERS',       # flipkart/orders
    '1T0BkL4p5Yhaqh63141l5P3Gb5Tp3dyxd': 'FK_RETURNS',       # flipkart/returns
    '1W05Pdgc_Fk7CbRIRUdtA6ZcTFM6SSrxz': 'FK_VIEWS',        # flipkart/views
    '1VlwkUbx6bzLi1fw1F3qbO_klfDM3vNth': 'FK_KEYWORDS',     # flipkart/keywords
    '1ZhNhUH0Yl4ingB830PEgt6pHfHoc1T2S': 'FK_ADS',          # flipkart/ads (master folder)
    '1NaZuJ0-TMLQxHyceCL2u-MwRT6DQZGAf': 'FK_ADS_DAILY',    # flipkart/ads/daily
    '19A4TFrqORQ-NpM3M0APljKFpVZ9Fj0_N': 'FK_ADS_FSN',      # flipkart/ads/fsn
    '1OouwwP4aVbAYkbCJe76zp2WOyfIN2G7o': 'FK_ADS_PLACEMENTS', # flipkart/ads/placements
    '1DpC5qI5_47QPxq_dda_Y1LV1UIaZf4SR': 'FK_ADS_OVERALL',   # flipkart/ads/overall
    '1fDvZU1SrJc4Ijixz-4vc_hMh7XYCtwCb': 'FK_ADS_SEARCH',   # flipkart/ads/search
    '1iNICRCucsPG-cJbAgQ_lq4nM_Oj-W6mG': 'FK_ADS_ORDERS',   # flipkart/ads/orders
    '1kCZKj09s3pqZTDtl8Q3dHC0LD8BL5O_T': 'FK_ADS_KW',       # flipkart/ads/keywords
    '1sBCegMtxLxr02RkvmlJ5OGYHfD_raBnU': 'FK_LISTINGS',     # flipkart/listings
    '1KY-M0_7_FDm_GlqMht4HO2w2wzPRkSgp': 'FK_PAYMENTS',     # flipkart/payments
    '1Ov-iVVqrl9KpCoZXUlqeV0tNTjGFQeD3': 'FK_CLAIMS',       # flipkart/claims
}

# Folders where the file is re-uploaded in place (same name, content grows by append).
# Dedup by Drive modifiedTime instead of filename.
_RECHECK_BY_MODTIME = {
    '1EMqTpDtsratSY66UbbrV4VsnGIXYKFqV',  # ME_VIEWS — meesho_views.csv
}

# File types that are not yet processed (skip downloading them)
_SKIP_TYPES = {
    'ME_ADS',      # parent folder — extension never uploads files directly here
}

# ─── DB Config Helpers (local, no import from process.py) ────────────────────

def _db_config_get(db, key, default=''):
    """Get a value from the DB config table."""
    for r in db.get('config', []):
        if r.get('key') == key:
            return str(r.get('value', default))
    return default


# ─── Drive Auth ───────────────────────────────────────────────────────────────

def _build_service():
    """
    Build a Google Drive API v3 service object.
    Tries GOOGLE_DRIVE_CREDENTIALS env var first, then credentials.json.

    Raises:
        ImportError   — google-api-python-client not installed
        FileNotFoundError — no credentials found at all
        Exception     — any other auth failure (caller catches generically)
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise ImportError(
            "Google API client not installed.\n"
            "Run:  pip install google-api-python-client google-auth"
        )

    creds_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    if creds_json:
        try:
            creds_info = json.loads(creds_json)
        except json.JSONDecodeError as e:
            raise ValueError(f"GOOGLE_DRIVE_CREDENTIALS is not valid JSON: {e}")
    else:
        creds_path = Path(__file__).parent / 'credentials.json'
        if not creds_path.exists():
            raise FileNotFoundError(
                "No Drive credentials found.\n"
                "  Option A: set GOOGLE_DRIVE_CREDENTIALS env var to service-account JSON string\n"
                "  Option B: place credentials.json in the project root (D:\\Claude RuMee Dashbord\\)"
            )
        with open(creds_path, encoding='utf-8') as f:
            creds_info = json.load(f)

    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=scopes
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)


# ─── Drive API Helpers ────────────────────────────────────────────────────────

def _list_folder_files(service, folder_id):
    """
    List all non-trashed files directly in a Drive folder.
    Returns list of dicts with keys: id, name, modifiedTime, mimeType.
    """
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    fields = 'nextPageToken, files(id, name, modifiedTime, mimeType)'

    while True:
        try:
            resp = service.files().list(
                q=query,
                fields=fields,
                pageToken=page_token,
                pageSize=100,
            ).execute()
        except Exception as e:
            raise RuntimeError(f"Drive list error for folder {folder_id}: {e}")

        files.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break

    return files


def _download_file(service, file_id, dest_path):
    """
    Download a Drive file to dest_path (overwrite if exists).
    Handles Google Workspace files (Sheets → xlsx, Docs → pdf) via export.
    """
    from googleapiclient.http import MediaIoBaseDownload

    # Check if it's a Google Workspace file that needs export
    meta = service.files().get(fileId=file_id, fields='mimeType').execute()
    mime = meta.get('mimeType', '')

    if mime == 'application/vnd.google-apps.spreadsheet':
        # Export as xlsx
        request = service.files().export_media(
            fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        dest_path = Path(str(dest_path).rstrip('.csv').rstrip('.xls') + '.xlsx')
    elif mime.startswith('application/vnd.google-apps.'):
        # Other Google Workspace types — skip
        print(f"  Drive: skipping Google Workspace file (mime={mime})")
        return None
    else:
        request = service.files().get_media(fileId=file_id)

    dest_path = Path(dest_path)
    with open(dest_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()

    return dest_path


# ─── Public API ───────────────────────────────────────────────────────────────

def fetch_new_files(db, temp_dir=None):
    """
    Scan all configured Drive folders and download files not yet processed.

    A file is considered "already processed" if:
        get_config(db, f'processed_file:{filename}') returns a non-empty string.

    Args:
        db:       loaded DB dict (used to check processed_file config keys)
        temp_dir: optional Path; if None, a system temp dir is created

    Returns:
        List of (local_path: Path, file_type_hint: str) pairs.
        Returns empty list if Drive auth fails (caller falls back to new_data/).
    """
    # Try to connect
    try:
        service = _build_service()
        print("  Drive: authenticated successfully")
    except ImportError as e:
        print(f"  Drive: {e}")
        print("  Falling back to local new_data/ folder.")
        return []
    except FileNotFoundError as e:
        print(f"  Drive: {e}")
        print("  Falling back to local new_data/ folder.")
        return []
    except Exception as e:
        print(f"  Drive: auth error — {e}")
        print("  Falling back to local new_data/ folder.")
        return []

    # Set up temp download directory
    if temp_dir is None:
        tmp = tempfile.mkdtemp(prefix='rumee_drive_')
        temp_dir = Path(tmp)
    else:
        temp_dir = Path(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

    results = []

    for folder_id, file_type_hint in DRIVE_FOLDERS.items():
        # Skip file types not yet implemented
        if file_type_hint in _SKIP_TYPES:
            continue

        try:
            files = _list_folder_files(service, folder_id)
        except Exception as e:
            print(f"  Drive: could not list folder {folder_id} ({file_type_hint}): {e}")
            continue

        if not files:
            continue

        # Sort alphabetically so monthly files (01_2026, 02_2026...) process oldest-first
        files.sort(key=lambda f: f['name'])

        for f in files:
            fname = f['name']
            ext = Path(fname).suffix.lower()

            # Only handle CSV and Excel files
            if ext not in ('.csv', '.xlsx', '.xls'):
                # Also handle Google Sheets (no extension)
                if f.get('mimeType') != 'application/vnd.google-apps.spreadsheet':
                    continue

            # Prefix with folder type to avoid name collisions between folders
            # e.g. both ME_PAYMENTS and FK_PAYMENTS can have "05_2026.xlsx"
            safe_name = f"{file_type_hint.lower()}_{fname}"
            if folder_id in _RECHECK_BY_MODTIME:
                # File is re-uploaded in place — dedup by modifiedTime
                last_mt  = _db_config_get(db, f'processed_modified:{safe_name}', default='')
                file_mt  = f.get('modifiedTime', '')
                if last_mt and file_mt and file_mt <= last_mt:
                    continue
            else:
                config_key = f'processed_file:{safe_name}'
                if _db_config_get(db, config_key, default=''):
                    continue
            local_path = temp_dir / safe_name
            try:
                actual_path = _download_file(service, f['id'], local_path)
                if actual_path is None:
                    continue  # Unsupported Workspace type
                size_kb = actual_path.stat().st_size // 1024
                print(f"  Drive: downloaded {fname} ({file_type_hint}, {size_kb} KB)")
                results.append((actual_path, file_type_hint, f.get('modifiedTime', '')))
            except Exception as e:
                print(f"  Drive: failed to download {fname}: {e}")

    if results:
        print(f"  Drive: {len(results)} new file(s) to process")
    else:
        print("  Drive: no new files found")

    return results


def test_auth():
    """
    Verify Drive credentials are valid by attempting authentication.
    Raises an exception if credentials are missing or invalid.
    Called from process.py --source=drive to fail fast before scanning folders.
    """
    _build_service()


def cleanup_temp_files(file_paths):
    """
    Delete downloaded temp files and their parent directory if now empty.
    Call this after all Drive files have been processed and DB saved.
    """
    dirs_to_try = set()
    for fp in file_paths:
        fp = Path(fp)
        dirs_to_try.add(fp.parent)
        try:
            fp.unlink(missing_ok=True)
        except Exception as e:
            print(f"  Drive cleanup: could not delete {fp.name}: {e}")

    for d in dirs_to_try:
        try:
            d.rmdir()  # Only removes if empty
        except Exception:
            pass
