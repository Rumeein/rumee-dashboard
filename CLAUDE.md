# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Session Start — Mandatory

Before doing any work, read these in order:
1. `D:\how-i-work\GOLDEN_RULES.md`
2. `C:\Users\jaisw\.claude\projects\D--Claude-RuMee-Dashbord\memory\MEMORY.md`
3. `active.md` and `context.md` from that memory folder
4. Surface any open (in-progress / blocked) items from `active.md` before proceeding

---

## Pipeline Commands

```bash
# Normal run (downloads new Drive files, processes, writes DB + Firestore)
python process.py

# Dry run — processes files but does NOT save DB or push to Firestore
python process.py --dry-run

# Full reset — clears all data and reprocesses everything from scratch
python process.py --reset-db

# Reset FK returns only (surgical backfill)
python process.py --reset-returns

# Regenerate all-time data (Firestore alltime doc)
python process.py --generate-alltime

# One-time Amazon catalog pull to Firestore
python push_az_catalog_firestore.py
```

Pipeline runs automatically via GitHub Actions every 6 hours (`.github/workflows/process_data.yml`).

**Check pipeline logs without asking Jaiswal:** Use the Actions read PAT from `context.md` — `GET https://api.github.com/repos/Rumeein/rumee-dashboard/actions/runs?per_page=5`. Never use the `contents:write` PAT from `index.html` for Actions — it returns 401.

---

## Architecture Overview

### The two codebases

| File | Role |
|---|---|
| `process.py` (~6 100 lines) | Python pipeline — runs in GitHub Actions. Downloads files from Google Drive, parses them, merges into DB CSVs, pushes to Firestore. |
| `index.html` (~8 100 lines) | Single-file dashboard — all HTML, CSS, JS in one file. Served as GitHub Pages. Reads from Firestore REST API only. |

These two never communicate directly. `process.py` writes; `index.html` reads.

### Data flow

```
Google Drive folders
    ↓ drive_connector.py (fetch_new_files)
process.py (file-type detection → per-platform processors)
    ↓ firestore_connector.py
Firestore (rumee-dashboard-6c4c6)
    ↓ Firestore REST API (FB_BASE in index.html)
index.html (GitHub Pages — public)
```

Local CSV files (`rumee_db_*.csv`) are intermediate — written by `process.py`, then pushed to Firestore. They are NOT in the public repo (moved to private `rumeein/rumee-data`).

### process.py internals

**File detection:** `detect_file_type()` sniffs the header row of each downloaded CSV/XLSX and routes to the correct processor.

**Per-platform processors** (all return row lists, never mutate DB directly):
- Meesho: `process_meesho_orders`, `process_meesho_returns`, `process_meesho_payments`, `process_meesho_ads`, `process_me_ads_summary`, `process_me_ads_catalog`, `process_me_views`
- Flipkart: `process_fk_payments`, `process_fk_orders`, `process_fk_returns`, `process_fk_listings`, `process_fk_views`, `process_fk_keywords`, `process_fk_ads`, `process_fk_ads_campaign`, `process_fk_ads_daily`, `process_fk_ads_kw`, `process_fk_ads_placements`
- Amazon: `process_az_monthly` (SP-API Orders v0), `pull_amazon_catalog.py` (standalone, not yet wired into `process.py`)
- Catalog: `process_catalog` — maps Meesho style names → `sku_id` via `ME_SKU_MAP`

**Watermarking:** every file type has a `*_last_date` config key in `db['config']`. Processors skip rows on or before that date, then update the watermark. Drive files are tracked by `processed_file:<file_id>` in config.

**Merge functions** combine new rows into existing DB: `merge_monthly`, `merge_me_skus`, `merge_fk_skus`, `merge_fk_keywords`, `merge_me_state_summary`, `merge_fk_zone_summary`.

**DB tables** (in `rumee_db_summary.csv`):
`config`, `fk_monthly`, `me_monthly`, `fk_skus`, `me_skus`, `me_return_reasons`, `fk_return_reasons`, `fk_pairs`, `az_monthly`, `fk_keywords`, `me_claims`, `fk_claims`, `fk_listings`

**Daily tables** (in `rumee_db_daily.csv`): `fk_daily`, `me_daily`, `fk_orders_daily`, `fk_orders_sku`, `fk_returns_daily`, `fk_returns_sku`

**Ads tables** (`rumee_db_fk_ads.csv`): `fk_ads_daily`, `fk_ads_sku`, `fk_ads_kw`, `fk_ads_placements`, `fk_ads_overall`, `fk_ads_search`, `fk_ads_order_items`

### index.html internals

**Global data object `D`** — all dashboard state. Initialized from `localStorage` key `rumee_v4`, falls back to hardcoded `DEF` constant. Persisted via `saveD()`.

**Load sequence on page open:**
1. `loadSummary()` — fetches `rumee_db/summary` from Firestore → `applyDB(db)` populates `D` → renders all monthly charts
2. `loadDailyData()` (background) — fetches `rumee_fk_daily/{YYYY_MM}`, `rumee_me_daily/{YYYY_MM}`, `rumee_fk_returns_daily/{YYYY_MM}` → `applyDailyDB(db)` populates `D.DAILY.*` → re-renders all views
3. Tab-specific loaders fire when user opens a tab: `loadFkAdsData()`, `loadKeywordsData()`, `loadAlltimeData()`, `renderTasksTab()`, `loadSchedule()`, `loadPipelineMap()`

**Firestore access pattern:** `fbGet(collection)` reads all docs; `fbPatch(collection, docId, fields)` writes. Both use the public `FB_API_KEY` (Firestore security rules limit access). No write path from the dashboard for business data — index.html is a public page.

**Firestore collections used by dashboard:**
- `rumee_db/summary` — main DB blob (CSV content in `content` field)
- `rumee_db/alltime` — all-time monthly history
- `rumee_fk_daily/{YYYY_MM}`, `rumee_me_daily/{YYYY_MM}`, `rumee_fk_returns_daily/{YYYY_MM}`
- `rumee_fk_ads_daily/{YYYY_MM}`, `rumee_fk_ads_kw/{YYYY_MM}`, `rumee_fk_ads_sku/{YYYY_MM}`
- `rumee_me_ads_daily/{YYYY_MM}`, `rumee_me_ads_catalog/{YYYY_MM}`
- `rumee_keywords/{YYYY_MM}`
- `rumee_insights` — AI insight cards (read + mark resolved)
- `rumee_tasks` — action items (read + update status)
- `product_master/{sku_id}` — catalog/product master (read + patch fk_url/me_url)
- `rumee_settings/schedule_{day}` — fulfilment schedule
- `rumee_az_catalog/{YYYY_MM}` — Amazon catalog listings

**Tabs:** Master, Flipkart, Meesho, Amazon, Products, Tasks, Data Files, Docs, Returns

**Returns tab** is self-contained — uses Google Identity Services OAuth (client ID in `localStorage`), writes scanned AWB barcodes to a Google Sheet. No server-side component.

---

## Connector Modules

| File | Purpose |
|---|---|
| `drive_connector.py` | Downloads new files from Google Drive folders using service account. Key fn: `fetch_new_files(db)` returns list of local temp paths. |
| `firestore_connector.py` | Writes processed data to Firestore. Key fns: `write_monthly_table`, `write_csv_content`, `write_insight`, `write_task`, `write_product_master_ids`. |
| `sheets_connector.py` | Reads/writes Orders Ledger Google Sheet. Key fns: `get_or_create_ledger`, `upsert_rows`, `fetch_return_receipts`. |

---

## Secrets & Credentials

**Never commit:** `rumee_secrets.py` (gitignored). Use `rumee_secrets.example.py` as template.

**GitHub Actions secrets:** `GOOGLE_DRIVE_CREDENTIALS`, `GMAIL_USER`, `GMAIL_APP_PASSWORD`, `AMAZON_LWA_CLIENT_ID`, `AMAZON_LWA_CLIENT_SECRET`, `AMAZON_REFRESH_TOKEN`, `DISCORD_WEBHOOK_URL_PIPELINE`, `FIREBASE_CREDENTIALS`.

**Hard rule — no write tokens in index.html.** index.html is public — any token embedded in it is visible to anyone. No `actions:write`, `contents:write`, or Sheets write tokens in client-side JS.

---

## Product Master — Current State (SCHEMA CHANGE PENDING)

**Do NOT touch `process_catalog`, `write_product_master_ids`, `write_az_product_master`, `pmWrite`, or any other Products tab code without reading `active.md` item #16 AND `DOCS.md §27` (Products Tab — Standing Invariants & Verification Checklist) first.**

**This is not optional and not a "read once" pointer — it applies every single time this area is touched, no matter how small the change looks.** `DOCS.md §27` exists specifically because a real, live-data-corrupting bug (the DJ-6 Undo incident, 2026-07-10) came from a fix in this exact area that looked safe and wasn't. The checklist there was built and tested precisely so the next change doesn't have to rediscover these failure patterns from scratch.

**Mandatory before considering ANY change to this area done:**
1. Read `DOCS.md §27`'s 10 invariants before writing code — they define what "correct" means here.
2. After the change, re-run the 3 verification snippets in `DOCS.md §27` (paste into the browser console — they mock Firestore, safe against live data) and confirm all three still print PASS.
3. If the change adds a new write path or a new Undo-style action, extend the checklist with the new pattern — don't leave the next session to rediscover it.

Target schema (approved 2026-07-01 — see DOCS.md §22 Decisions and active.md #16 for full spec): one Firestore doc per variation (`sku_id`) with an embedded `listings[]` array (not separate docs per listing).

**Product hierarchy — always 3 levels, no exceptions:**
- **Design family** (e.g. "DJ-7", "Bangle", "Necklace") — level 1, free text
- **Variation** — level 2, free text label. NOT restricted to og/bahubali/base. Examples: `bahubali`/`og` for earrings, `Bangle-4`/`Bangle-5` for bangles, `Oxidized`/`Gold`/`Long`/`Short` for necklaces
- **Listing** — level 3, individual Meesho/Flipkart/Shopsy/Amazon catalog entry

**DEPRECATED — do not use.** The rules below (og/bahubali/base as a fixed enum, keyword auto-classification) are superseded. Unmapped SKUs now go to a `needs_review` Firestore collection for manual assignment via the dashboard, not automatic keyword guessing.

~~**Auto-detection rules for `variation_type`:**~~
~~- `style_id` starts with "OG" → `og`~~
~~- Contains "Bahubali" or no OG prefix → `bahubali`~~
~~- Bangles/necklaces/combos (keyword match) → `base`~~
~~- Unknown series: ask Jaiswal "Did you attach the chain yourself?"~~

---

## Key Conventions

- **Column rename over drop.** If a column has empty/zero data, rename it to something self-explanatory. Never drop — Jaiswal may populate it later.
- **Watermark pattern.** Every file type has a `*_last_date` in `db['config']`. Always check and update watermarks. Never re-process already-ingested rows.
- **Drive-first.** Pipeline reads from Google Drive API only — never local file paths or G: drive.
- **Returns Scanner badge** in `index.html` at `#ret-build` span (~line 1446). Format: `BUILD YYYY-MM-DD · vN · short description`. Bump version on every push touching scanner code. Tell Vishal the exact badge string to look for after pushing.
- **Global build badge** in `index.html` at `#siteBuild` span (in the main `<header class="site-hdr">`, visible on every tab — added 2026-07-05 so Jaiswal can visually confirm when a push is actually live). Format: `BUILD YYYY-MM-DD HH:MM IST` — **IST (UTC+5:30), not UTC** (Jaiswal's explicit preference 2026-07-06, easier for him to recall than UTC; this environment's `date` command doesn't honor `TZ=`, so compute IST manually as UTC+5:30 rather than trusting `TZ=Asia/Kolkata date`). **Update this timestamp on every push that changes `index.html`** — it's a plain hardcoded string, not auto-generated. Tell Vishal the exact badge string after pushing so he can compare it against what he sees in the browser (and knows to hard-refresh if it hasn't updated yet, since GitHub Pages/browser caching can lag).
- **Discord notifications** in `process.py`: `send_discord_notification()` fires end of every run. Env var: `DISCORD_WEBHOOK_URL_PIPELINE`.

---

## Open Work (summary — read active.md for full detail)

| # | Item | Status |
|---|---|---|
| 1 | Insights grouping (dedup by text, badge N SKUs) | in-progress |
| 6 | FK Orders Dashboard UI (5 edit points in index.html) | in-progress |
| 14 | Orders Ledger — waiting for new FK_PAYMENTS file | in-progress |
| 16 | Products Tab catalog processing redesign | redesign shipped + fragmentation cleanup verified 2026-07-10 — see mandatory checklist above (DOCS.md §27) before any further change |
| 17 | Amazon catalog Firestore — pending data validation | in-progress |
| 18 | Amazon pipeline security audit | not started |
