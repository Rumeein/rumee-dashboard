# Rumee Dashboard — Complete Project Documentation

> **Who this is for:** Any developer or AI assistant working on this project. You should be able to understand the entire system from this file without reading the code or asking the owner.
>
> **Rule:** When any decision changes, this file must be updated in the same session it changes.

Last updated: 2026-06-20

---

## Table of Contents

1. [What This Project Does](#1-what-this-project-does)
2. [Three Products — How They Relate](#2-three-products--how-they-relate)
3. [Data Flow — End to End](#3-data-flow--end-to-end)
4. [Design Principle: No Local Machine](#4-design-principle-no-local-machine)
5. [Pipeline — process.py](#5-pipeline--processpy)
6. [Google Drive Authentication](#6-google-drive-authentication)
7. [GitHub Actions — Pipeline Automation](#7-github-actions--pipeline-automation)
8. [Dashboard — index.html](#8-dashboard--indexhtml)
9. [Vantage — AI Growth Advisor](#9-vantage--ai-growth-advisor)
10. [File Structure](#10-file-structure)
11. [Build Status](#11-build-status)
12. [Key Decisions](#12-key-decisions)

---

## 1. What This Project Does

Rumee Dashboard processes raw seller data from Flipkart and Meesho, stores it in GitHub as clean CSV files, and displays it in a visual dashboard hosted on GitHub Pages. It is the data layer and UI for the full Rumee Growth System.

**Seller:** Rumee Jewellery (rumeein@gmail.com) — artificial jewellery on Flipkart and Meesho.

---

## 2. Three Products — How They Relate

| Product | Repo | Purpose |
|---|---|---|
| Chrome Extension (AutoSync) | rumee-auto-sync | Captures raw data from seller panels → uploads to Google Drive |
| Dashboard + Pipeline | rumee-dashboard (this repo) | Reads Drive → processes → clean DB CSVs → visual dashboard |
| Vantage | vantage-agent (generic runner) + rumee-dashboard/vantage/ (instance) | AI growth advisor — reads DB, suggests experiments, tracks outcomes |

All three are decoupled. Extension → Drive → Pipeline → GitHub → Vantage. Each step is independent.

---

## 3. Data Flow — End to End

```
[Seller Panel: Flipkart / Meesho]
        ↓  Chrome Extension (AutoSync) captures data automatically
[Google Drive]
    Raw files, one per report per day, organised in per-platform folders
        ↓  process.py reads via Drive API (Service Account — headless)
        ↓  runs on GitHub Actions on schedule
[GitHub repo: rumee-dashboard]
    rumee_db_summary.csv  — all summary tables (fk_monthly, me_monthly, fk_skus, me_skus, etc.)
    rumee_db_daily.csv    — per-SKU daily rows
    rumee_db_keywords.csv — Flipkart keyword data
    rumee_db_alltime.csv  — all-time cumulative data
    index.html            — dashboard (auto-updates on commit)
        ↓  GitHub Pages serves index.html
[Dashboard — rumeein.github.io/rumee-dashboard]
    Reads CSVs via GitHub raw URLs (no server, pure static)
        ↓  Vantage fetches same CSVs
[Vantage]
    Reads DB CSVs from GitHub raw URLs
    Writes experiments, learnings, activity log back to GitHub repo
```

**GitHub raw URLs Vantage uses:**
- `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_summary.csv`
- `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_daily.csv`

---

## 4. Design Principle: No Local Machine

**Decided:** Nothing in the data processing pipeline should depend on or require the local Windows machine.

| Step | Runs where |
|---|---|
| Data capture (AutoSync extension) | Browser — user's machine, but only while capturing |
| Pipeline (process.py) | GitHub Actions — scheduled, fully cloud |
| Dashboard | GitHub Pages — static, no server |
| Vantage nightly analysis | GitHub Actions — scheduled, fully cloud |
| Vantage Discord Q&A | Cloud server (Fly.io or equivalent) — 24/7 |
| Vantage memory (experiments, learnings, activity log) | GitHub repo — committed after every write |

**Why:** Local machine = single point of failure. Everything on GitHub/cloud = nothing is lost if the PC is off, reset, or replaced.

---

## 5. Pipeline — process.py

`process.py` is the core data processing script. It:

1. Reads new files from Google Drive folders (via `drive_connector.py`)
2. Detects file type from folder ID mapping (see `drive_connector.py` → `DRIVE_FOLDERS`)
3. Parses each file (CSV/XLSX) into structured records
4. Merges with existing DB CSVs
5. Writes updated `rumee_db_summary.csv`, `rumee_db_daily.csv`, etc.
6. (Via GitHub Actions) commits and pushes updated CSVs to the repo

**Key files:**
| File | Purpose |
|---|---|
| `process.py` | Main pipeline — orchestrates all handlers |
| `drive_connector.py` | Google Drive API — fetches new files from Drive folders |
| `rumee_db_summary.csv` | All summary tables in one CSV (table name in column 0) |
| `rumee_db_daily.csv` | Per-SKU daily rows |
| `rumee_db_keywords.csv` | FK keyword data |
| `rumee_db_alltime.csv` | All-time cumulative |

**Supported data streams (handlers in process.py):**

| Stream | Platform | Status |
|---|---|---|
| ME_ORDERS | Meesho | Done |
| ME_RETURNS | Meesho | Done |
| ME_PAYMENTS | Meesho | Done |
| ME_ADS (master, summary, catalog) | Meesho | Done |
| ME_VIEWS | Meesho | Done |
| ME_CLAIMS | Meesho | Done |
| CATALOG | Meesho | Done |
| FK_PAYMENTS | Flipkart | Done |
| FK_VIEWS | Flipkart | Done |
| FK_KEYWORDS | Flipkart | Done |
| FK_LISTINGS | Flipkart | Done |
| FK_CLAIMS | Flipkart | Done |
| FK_ADS_* (daily, fsn, placements, overall, search, orders, kw) | Flipkart | Pending |
| FK_ORDERS | Flipkart | Pending |
| FK_RETURNS (reason breakdown) | Flipkart | Pending |

---

## 6. Google Drive Authentication

**Method: Service Account (not OAuth2)**

`drive_connector.py` uses a Google service account — a JSON key that works headlessly with no browser login required.

**Auth priority in drive_connector.py:**
1. `GOOGLE_DRIVE_CREDENTIALS` environment variable (JSON string) — used by GitHub Actions (stored as GitHub Secret)
2. `credentials.json` in project root — used for local testing

**Current state:** `credentials.json` in the repo root IS a service account key (type: `service_account`). GitHub Actions just needs this JSON stored as the `GOOGLE_DRIVE_CREDENTIALS` secret — no new Google Cloud setup required.

**The extension uses different auth:** AutoSync uses `chrome.identity.getAuthToken` (OAuth2 via Chrome) — completely separate from the pipeline auth. Do not confuse the two.

---

## 7. GitHub Actions — Pipeline Automation

**Decision:** Pipeline runs on GitHub Actions on a schedule. No local machine involvement.

**Workflow file to create:** `.github/workflows/pipeline.yml`

**What the workflow does:**
1. Checks out the repo
2. Sets up Python
3. Installs dependencies (`pip install -r requirements.txt`)
4. Runs `process.py` — reads from Drive, generates updated CSVs
5. Commits changed `rumee_db_*.csv` files
6. Pushes to `main`

**GitHub Secrets needed:**
| Secret name | Value |
|---|---|
| `GOOGLE_DRIVE_CREDENTIALS` | Full contents of `credentials.json` (service account JSON) |

**Status: DONE (2026-06-20) — `.github/workflows/pipeline.yml` committed. Requires `GOOGLE_DRIVE_CREDENTIALS` secret added in GitHub repo settings.**

---

## 8. Dashboard — index.html

Single-file static dashboard hosted on GitHub Pages.

**URL:** `https://rumeein.github.io/rumee-dashboard/`

**Data loading:** `index.html` fetches CSVs from GitHub raw URLs on page load — no server, no build step, no backend.

**Tabs:**
| Tab | What it shows |
|---|---|
| Master | Combined view — GMV, orders, returns across platforms |
| Flipkart | FK monthly + SKU + ads data |
| Meesho | ME monthly + SKU + views + return reasons |
| Amazon | Placeholder — not built |
| Tasks | Open tasks, pulled from Firebase Firestore |
| Dev | Dev board — pulled from memory files, auto-updated by hook |
| Data Pipeline | 15 data streams, gap detection, Vantage wishlist badge |
| Returns | Returns reconciliation tab — spec written, not built |

**Backend storage:** Firebase Firestore (Spark plan, free, never pauses)
- Project ID: stored in `index.html` constants
- Used for: Tasks, Insights (not for DB data — that's GitHub CSVs)

---

## 9. Vantage — AI Growth Advisor

### What it does

Runs nightly analysis on the business DB, generates alerts and experiment suggestions, tracks outcomes, and answers questions on Discord.

### Repos

| Repo | Path | Purpose |
|---|---|---|
| vantage-agent | `D:\vantage-agent\` | Generic product — runner, system prompt, shared learnings |
| Rumee instance | `D:\Claude RuMee Dashbord\vantage\` | Business config, memory (experiments, learnings, activity log) |

### Data source (decided 2026-06-20)

Vantage reads processed data from GitHub raw URLs — same CSVs the dashboard uses. No local file access.

**`context_builder.py` must fetch from:**
- `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_summary.csv`
- `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_daily.csv`

**Status: NOT YET IMPLEMENTED** — context_builder.py currently reads local file path. Needs to switch to `requests.get()`.

### Memory storage (decided 2026-06-20)

Vantage writes experiments, learnings, and activity log to the GitHub repo (committed + pushed after every write). Nothing stored only on local disk.

**Files:**
- `vantage/memory/experiments.json`
- `vantage/memory/learnings.json`
- `vantage/memory/activity_log.jsonl`

**Status: NOT YET IMPLEMENTED** — currently writes to local disk only.

### LLM

- Provider: Groq (free, no credit card)
- Model: `llama-3.3-70b-versatile`
- API key: `GROQ_API_KEY` in `vantage/.env` (gitignored)

### Discord bot

- Bot: vantage#8332 | App ID: 1517859731539234826
- Channel: 1517718649429954691 (#pipeline on Rumee Discord server)
- Commands: `!status`, `!alerts`, free-form Q&A
- Run: `python discord_bot.py --instance-path "D:\Claude RuMee Dashbord\vantage"`
- **Status: Built and tested. Not yet hosted on cloud server.**

### fk_skus data schema (important — prevents hallucination)

`fk_skus` contains **ad performance data only** — NOT order or return counts per SKU.

| Column | Meaning |
|---|---|
| `ad_attributed_revenue_rs` | Revenue (₹) from ad-driven sales |
| `units_sold_via_ads` | Units sold via ads |
| `ad_impressions` | Times the ad was shown |
| `revenue_earned_rs` | Settlement payout from Flipkart |

`stock` column dropped — all zeros, misleads LLM.

Per-SKU FK orders/returns do not exist in the DB — only monthly totals in `fk_monthly`.

---

## 10. File Structure

```
rumee-dashboard/
├── index.html              — full dashboard (single file)
├── process.py              — pipeline: reads Drive, writes DB CSVs
├── drive_connector.py      — Google Drive API wrapper (service account auth)
├── rumee_db_summary.csv    — all summary tables
├── rumee_db_daily.csv      — per-SKU daily rows
├── rumee_db_keywords.csv   — FK keyword data
├── rumee_db_alltime.csv    — all-time cumulative
├── credentials.json        — service account key (gitignored)
├── DOCS.md                 — this file
├── ARCHITECTURE.md         — superseded by DOCS.md
├── vantage/
│   ├── business_profile.json   — Vantage config for Rumee instance
│   ├── .env                    — GROQ_API_KEY, DISCORD_BOT_TOKEN (gitignored)
│   └── memory/
│       ├── experiments.json
│       ├── learnings.json
│       └── activity_log.jsonl
└── rumee-extension/
    ├── DOCS.md             — extension documentation (needs committing)
    └── ...
```

---

## 11. Build Status

| Component | Status |
|---|---|
| Extension — Flipkart + Meesho capture | Done |
| Extension — Amazon | Not started |
| Pipeline — Drive API + all ME handlers | Done |
| Pipeline — FK core handlers (payments, views, keywords, listings, claims) | Done |
| Pipeline — FK_ADS_*, FK_ORDERS, FK_RETURNS reasons | Pending |
| Pipeline on GitHub Actions (service account auth) | **Done (2026-06-20) — needs GOOGLE_DRIVE_CREDENTIALS secret added** |
| Dashboard — core metrics (FK + ME) | Done |
| Dashboard — Returns tab | Spec written, not built |
| Dashboard — Deep Dive tab (experiment board) | Design done, not built |
| Vantage — runner, context builder, LLM, Discord bot | Done |
| Vantage — data standardization (fk_skus rename) | Done (2026-06-20) |
| Vantage — context_builder reads from GitHub URLs | **Not yet implemented** |
| Vantage — memory writes to GitHub repo | **Not yet implemented** |
| Vantage — nightly run on GitHub Actions | **Not yet implemented** |
| Vantage — Discord Q&A on cloud server (24/7) | **Not yet implemented** |
| Vantage — eval loop (automated training) | Not started — after GitHub Actions |

---

## 12. Key Decisions

| Decision | What was decided | Date |
|---|---|---|
| No local machine in pipeline | Pipeline runs on GitHub Actions. Nothing requires the local PC after data capture. | 2026-06-20 |
| DB storage | DB CSVs committed to GitHub repo — GitHub is the cloud storage for processed data | — |
| Raw data storage | Google Drive — extension uploads directly, organised by folder per stream | — |
| Drive auth | Service account (`credentials.json`) — works headlessly. Already in place. GitHub Actions uses `GOOGLE_DRIVE_CREDENTIALS` secret. | — |
| Pipeline trigger | GitHub Actions on schedule (daily) — no manual step | 2026-06-20 |
| Vantage data source | Reads from GitHub raw URLs (same CSVs as dashboard). No local file access. | 2026-06-20 |
| Vantage memory | experiments.json, learnings.json, activity_log.jsonl committed to GitHub repo after every write | 2026-06-20 |
| LLM for Vantage | Groq (free) — llama-3.3-70b-versatile | — |
| Vantage 24/7 | Discord Q&A bot hosted on cloud server (Fly.io or equivalent). Nightly audit via GitHub Actions. | 2026-06-20 |
| fk_skus columns | Renamed in context_builder for clarity — ad_revenue → ad_attributed_revenue_rs, conversions → units_sold_via_ads, stock dropped | 2026-06-20 |
| Firebase | Firestore (Spark plan) used for Tasks and Insights only — not for DB data | — |
| Reusability | All three products generic — any seller can plug in their own data | — |
