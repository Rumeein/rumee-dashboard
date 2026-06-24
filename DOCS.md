# Rumee Dashboard — Complete Project Documentation

> **Who this is for:** Any developer or AI assistant working on this project. You should be able to understand the entire system from this file without reading the code or asking the owner.
>
> **Rule:** When any decision changes, this file must be updated in the same session it changes.

Last updated: 2026-06-23 (Section 15 added: full SP-API compliance & security framework from official Amazon docs)

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
9. [Vantage — Integration Reference](#9-vantage--integration-reference)
10. [Secrets Management](#10-secrets-management)
11. [File Structure](#11-file-structure)
12. [Build Status](#12-build-status)
13. [Amazon SP-API Integration](#13-amazon-sp-api-integration)
14. [Key Decisions](#14-key-decisions)
15. [Amazon SP-API — Full Compliance & Security Framework](#15-amazon-sp-api--full-compliance--security-framework)

---

## 1. Product Vision

**This is a generic, reusable product suite — not a tool built only for Rumee.**

Rumee Jewellery is the first business running on this system — the reference implementation. Every design decision has been made with replicability in mind. Any ecommerce seller on Flipkart, Meesho, or Amazon can plug in their own data and run the same system without writing new code.

**The three products are fully independent and multi-tenant by design:**

| Product | Generic? | What changes per business |
|---|---|---|
| Chrome Extension (AutoSync) | Yes | Google Drive folder IDs in `config.js` |
| Dashboard + Pipeline | Yes | GitHub repo, Drive folder IDs in `drive_connector.py` |
| Vantage AI Advisor | Yes | `business_profile.json` — name, stage, platforms, focus |

**Monetisation path:** Any seller can self-host for free (GitHub + Drive + Groq are all free tiers). A managed version — where we host and operate the system for other sellers — is a viable paid product built on the same codebase.

**Why this matters for development decisions:** Every feature built should work for any seller, not just Rumee. Rumee-specific config (folder IDs, webhook URLs, repo names) lives only in config files and environment variables — never hardcoded into the product code.

---

## 2. What This Repo Does

Rumee Dashboard processes raw seller data from Flipkart and Meesho, stores it in GitHub as clean CSV files, and displays it in a visual dashboard hosted on GitHub Pages. It is the data layer and UI for the full Rumee Growth System.

**Current deployment:** Rumee Jewellery (rumeein@gmail.com) — artificial jewellery on Flipkart and Meesho.

---

## 3. Three Products — How They Relate

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

**Workflow file:** `.github/workflows/process_data.yml` (already exists, already has run history)

**Schedule:** Every 6 hours (00:00, 06:00, 12:00, 18:00 UTC)

**Manual trigger options (from GitHub Actions UI):**
- `reset_db` — full reset and rebuild from scratch
- `generate_alltime` — regenerate the all-time data file

**What the workflow does:**
1. Checks out the repo
2. Installs dependencies
3. Writes `credentials.json` from `GOOGLE_DRIVE_CREDENTIALS` secret
4. Runs `python process.py --source=drive`
5. Detects if any DB CSV changed — skips commit if nothing changed
6. Commits and pushes changed files
7. Cleans up `credentials.json` (always runs, even on failure)

**GitHub Secrets needed (all already set):**
| Secret | Purpose |
|---|---|
| `GOOGLE_DRIVE_CREDENTIALS` | Service account JSON for Drive API |
| `GMAIL_USER` | Pipeline email notifications |
| `GMAIL_APP_PASSWORD` | Pipeline email notifications |

**Status: DONE — workflow live, running once daily at 6:30 PM IST (13:00 UTC).**

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
| Amazon | Placeholder — not built. `az_monthly` schema ready. SP-API registration under review (2026-06-22). |
| Tasks | Open tasks, pulled from Firebase Firestore |
| Dev | Dev board — pulled from memory files, auto-updated by hook |
| Data Pipeline | 15 data streams, gap detection, Vantage wishlist badge |
| Returns | Returns reconciliation tab — spec written, not built |

**Backend storage:** Firebase Firestore (Spark plan, free, never pauses)
- Project ID: stored in `index.html` constants
- Used for: Tasks, Insights (not for DB data — that's GitHub CSVs)

---

## 9. Vantage — Integration Reference

Vantage is a separate project (`D:\vantage-agent\`). Full documentation: `D:\vantage-agent\DOCS.md`.

**What this repo provides to Vantage:**

| Resource | GitHub raw URL |
|---|---|
| Summary DB | `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_summary.csv` |
| Daily DB | `https://raw.githubusercontent.com/Rumeein/rumee-dashboard/main/rumee_db_daily.csv` |

Vantage writes memory files back into this repo at `vantage/memory/` — committed and pushed after every run.

---

## 10. Secrets Management

**All secrets live in `rumee_secrets.py`** — a single file that is gitignored and never committed. It exists only on the local machine.

### File: `rumee_secrets.py` (gitignored — local only)

```python
FIREBASE_API_KEY = 'AIzaSy...'          # Firebase web API key (Firestore access)
DISCORD_WEBHOOK_URL = 'https://...'     # Rumee Dashboard Discord webhook
FLIPKART_API_SECRET = '12b66...'        # Flipkart Seller API secret (server-side use)
```

### File: `rumee_secrets.example.py` (committed — placeholder values only)

Template for setting up on a new machine. Copy to `rumee_secrets.py` and fill in real values.

### How each secret is used

| Secret | Used by | How |
|---|---|---|
| `FIREBASE_API_KEY` | `seed_product_master.py` | `from rumee_secrets import FIREBASE_API_KEY` |
| `DISCORD_WEBHOOK_URL` | `process.py` (pipeline summary + wishlist functions) | `from rumee_secrets import DISCORD_WEBHOOK_URL` |
| `FLIPKART_API_SECRET` | Future `process.py` FK API integration | `from rumee_secrets import FLIPKART_API_SECRET` |

### Firebase API key in `index.html`

`index.html` also contains the Firebase API key (line ~1815) hardcoded. This is **intentional and correct** — it is a client-side web API key, public by design. Firebase security is enforced by Firestore Security Rules, not by hiding the key. GitHub secret scanning flags it as a false positive — dismiss the alert in GitHub's Security tab.

### Setting up on a new machine

```
1. Copy rumee_secrets.example.py → rumee_secrets.py
2. Fill in real values (get from Jaiswal or password manager)
3. Never commit rumee_secrets.py
```

### Why this pattern exists

Before June 2026, secrets were hardcoded in `seed_product_master.py` and `process.py` and committed to the public GitHub repo. GitHub Secret Scanning and GitGuardian flagged them repeatedly. After the third incident, all secrets were moved to this gitignored file pattern permanently.

---

## 11. File Structure

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
├── rumee_secrets.py        — all secrets: Firebase key, Discord webhook, FK API secret (gitignored — local only)
├── rumee_secrets.example.py — template with placeholder values (committed)
├── DOCS.md                 — this file (single source of truth)
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

## 12. Build Status

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
| Vantage — context_builder reads from GitHub URLs | Done (2026-06-20) |
| Vantage — memory writes to GitHub repo | Done (2026-06-20) |
| Vantage — nightly run on GitHub Actions | **Not yet implemented** |
| Vantage — Discord Q&A on cloud server (24/7) | **Not yet implemented** |
| Vantage — eval loop (automated training) | Not started — after GitHub Actions |

---

## 13. Amazon SP-API Integration

### Registration Status (as of 2026-06-22)

| Item | Status |
|---|---|
| Developer portal | [developer.amazonservices.com](https://developer.amazonservices.com) |
| Account type | Private developer (our own store only — no Appstore listing needed) |
| App name | Rumee Dashboard |
| App ID | `amzn1.sp.solution.2f7d6de2-749e-4962-8849-d935e040df62` |
| App status | **Sandbox** — under review for production |
| Identity verification | Submitted 2026-06-22 |
| Developer profile | Submitted 2026-06-22 — Amazon review pending (3–14 days) |

### Roles Requested

| Role | Purpose |
|---|---|
| Product Listing | Create/update listings, manage A+ content |
| Pricing | Monitor and update product prices |
| Buyer Communication | Respond to return requests and customer queries |
| Buyer Solicitation | Request reviews and feedback post-order |
| Selling Partner Insights | Account performance, account health data |
| Finance and Accounting | Settlement reports, revenue statements |
| Inventory and Order Tracking | Order status, stock levels |
| Brand Analytics | Sales and inventory analytics for restocking decisions |

### Security Commitments Made to Amazon

These were declared in the Solution Provider Profile on 2026-06-22. **These are binding commitments — they must be maintained and monitored.**

| Commitment | What it means in practice |
|---|---|
| Firewalls, anti-virus, network security | Windows Defender active on all machines handling Amazon data. Router firewall enabled. |
| Access restricted by job role | Only the owner (Jaiswal) accesses Amazon data — no shared credentials |
| Amazon data encrypted in transit | All API calls over HTTPS only. Dashboard on GitHub Pages (HTTPS only). No HTTP. |
| Security incidents reported within 24 hours | Any breach or unauthorised access must be reported to security@amazon.com within 24 hours |
| Credentials stored securely | All credentials in gitignored `rumee_secrets.py` — never committed to GitHub. No hardcoding. |
| No third parties receive Amazon data | Amazon data stays internal — never shared with external services except GitHub (hosting) |
| No external non-Amazon sources for Amazon data | Amazon data comes only from SP-API — no scraping, no third-party data providers |

### What Amazon Data Will Flow Into

- `az_monthly` table in `rumee_db_summary.csv` — schema already defined in `process.py`
- Columns: `month | label | gmv | orders | ad_spend`
- Dashboard Amazon tab: placeholder exists in `index.html` — not yet built

### Next Steps (post-approval)

1. Amazon emails approval → complete identity verification step
2. Promote "Rumee Dashboard" app from Sandbox → Production in developer portal
3. Build SP-API handler in `process.py` to populate `az_monthly`
4. Build Amazon tab UI in `index.html`

### Programmatic Security Monitoring (pending — separate session)

A monitoring system must be built to verify all security commitments above are being met. Failures must trigger immediate Discord notification. This is a separate session task — see memory for spec.

---

## 15. Amazon SP-API — Full Compliance & Security Framework

> **Why this section exists:** This is the authoritative record of every legal and security obligation that comes with our Amazon SP-API access. It is the single place to check before any Amazon integration decision. Never let a session pass without reading this if Amazon data is being touched.
>
> **Policy baseline:** Amazon Data Protection Policy (DPP) + Acceptable Use Policy (AUP) + Solution Provider Agreement — effective November 25, 2025. All three are binding. Continued use of SP-API = acceptance.
>
> **Source:** [Amazon SP-API Policies and Agreements](https://developer-docs.amazon.com/sp-api/docs/policies-and-agreements)

---

### 15.1 Policies That Bind Us

| Policy | What it covers | Link |
|---|---|---|
| Data Protection Policy (DPP) | How Amazon data must be stored, encrypted, retained, and deleted | [DPP](https://developer-docs.amazon.com/sp-api/docs/policies-and-agreements) |
| Acceptable Use Policy (AUP) | What we can and cannot do with Amazon data | [AUP](https://developer-docs.amazon.com/sp-api/docs/policies-and-agreements) |
| Solution Provider Agreement | Legal terms — termination, modifications, liability | [Agreement](https://developer-docs.amazon.com/sp-api/docs/policies-and-agreements) |

---

### 15.2 Data Classification

Amazon data we will access falls into two categories with different rules:

| Category | Definition | Examples | Retention Limit |
|---|---|---|---|
| PII (Personally Identifiable Information) | Data that can identify a buyer | Buyer name, address, phone, email | **30 days after order delivery** |
| Non-PII | Business/operational data | Order totals, GMV, ad spend, impressions | **18 months maximum** |

**For Rumee specifically:**
- `az_monthly` table stores aggregated GMV, orders, ad_spend — no PII. 18-month cap applies.
- If we ever access buyer addresses or names via the Orders API — RDT required + 30-day deletion.

---

### 15.3 Restricted Data Tokens (RDT) — PII Access Rules

Certain SP-API operations return PII and are **restricted operations**. They require a Restricted Data Token (RDT) — not just a standard LWA access token.

**How to get an RDT:** Call `createRestrictedDataToken` via the Tokens API, passing the LWA token. Use the RDT in `x-amz-access-token` header for that call only.

**Rules:**
- RDTs cannot be used for standard (non-restricted) API calls
- RDTs must be handled with the same security as credentials
- PII obtained via RDT must be deleted within 30 days of order delivery
- PII must be encrypted at rest (AES-128+) if stored at all during those 30 days

**Affected roles we hold:**
| Role | May involve PII? | Action |
|---|---|---|
| Inventory and Order Tracking | Yes — buyer address in orders | Use RDT; delete within 30 days |
| Buyer Communication | Yes — buyer contact info | Use RDT; delete within 30 days |
| Buyer Solicitation | Yes — buyer contact info | Use RDT; delete within 30 days |
| Finance and Accounting | No — settlement data only | Standard LWA |
| Brand Analytics | No — aggregated analytics | Standard LWA |
| Selling Partner Insights | No — account performance | Standard LWA |
| Product Listing | No — catalog data | Standard LWA |
| Pricing | No — price data | Standard LWA |

**Rule: Do NOT store buyer names, addresses, or contact details in any CSV, Firestore, or GitHub file. Pull them on-demand with RDT and discard.**

---

### 15.4 Acceptable Use — What We CANNOT Do

| Prohibited | Detail |
|---|---|
| Share Amazon data with third parties | Data stays internal. Not passed to any external service, tool, or person. |
| Use non-Amazon sources for Amazon data | All Amazon data comes from SP-API only — no scraping, no third-party providers |
| Store Amazon data on personal devices | No phone storage, no personal laptop, no removable USB |
| Store PII on removable media without encryption | AES-128+ mandatory if ever done |
| Use generic/shared/default credentials | Every account must have unique credentials |
| Leave vulnerabilities unpatched | Critical: fix within 7 days. High: fix within 30 days. |
| Disable antivirus software | Windows Defender must stay active and cannot be user-disabled |
| Use LWA tokens to retrieve PII (deprecated) | LWA tokens no longer retrieve PII — must use RDT (discontinued Nov 2024) |

---

### 15.5 Security Controls — Full Requirements vs Our Status

#### Authentication & Passwords

| Requirement | Standard | Our Status | Action Needed |
|---|---|---|---|
| Password complexity | Min 12 chars, mixed case, numbers, special chars, no username components | Verify for Amazon Seller Central + developer portal | Audit passwords |
| Password history | Cannot reuse last 10 passwords | Verify | |
| Password max age | 365 days | Verify | |
| MFA | Mandatory — TOTP, hardware token, or biometric | Enable on Seller Central + developer portal | Enable MFA |
| Account lockout | Max 10 failed attempts | Platform-enforced (Amazon side) | N/A — Amazon enforces |
| API key rotation | Annual minimum, with automated processes | Not yet scheduled | Schedule annually |

#### Credential Storage

| Requirement | Standard | Our Status |
|---|---|---|
| No hardcoded credentials | Never in source code | Done — rumee_secrets.py pattern |
| Encrypted credential storage | AES-128 minimum | Done — OS-level encryption (Windows) |
| No plain text API keys | Never exposed in logs or output | Review process.py + index.html |
| Credentials in gitignored file | Must not be committed | Done — rumee_secrets.py gitignored |

#### Network & Encryption in Transit

| Requirement | Standard | Our Status |
|---|---|---|
| TLS version | TLS 1.2 minimum | Done — GitHub Pages + SP-API both enforce |
| Firewall | Network firewalls required | Done — router firewall active |
| Anti-malware | Antivirus on all systems accessing Amazon data | Done — Windows Defender |
| Monthly anti-malware updates | Defender signatures updated monthly minimum | Windows auto-updates — verify is on |
| IDS/IPS | Intrusion detection/prevention | Windows Defender covers this for our scale |
| Network segmentation | VLANs or subnets for isolation | N/A — home network; Defender + firewall sufficient for private developer |

#### Encryption at Rest

| Requirement | Standard | Our Status | Action |
|---|---|---|---|
| PII encryption | AES-128+ or RSA-2048+ | No PII stored at rest (aggregated only) | Maintain — never store PII |
| Key encryption at rest | AES-128+ | OS-level BitLocker on Windows | Verify BitLocker is on |
| Backup encryption | AES-128+ | GitHub repo is the backup — HTTPS + GitHub's encryption | Covered |

#### Data Retention

| Data Type | Limit | Our Policy | Status |
|---|---|---|---|
| PII | 30 days post-delivery | Do not store PII at all | Compliant |
| Non-PII (az_monthly etc.) | 18 months maximum | az_monthly: rolling aggregated data | Must implement 18-month purge |
| Security logs | Minimum 12 months | GitHub Actions logs retained by GitHub | Verify retention settings |
| Deleted data method | NIST 800-88 compliant | GitHub delete = acceptable (API delete) | Compliant |

#### Logging & Monitoring

| Requirement | Frequency | Our Status | Action |
|---|---|---|---|
| Log retention | 12 months minimum | GitHub Actions logs | Check GitHub log retention settings |
| Log review | Bi-weekly OR real-time automated | Not implemented | Add to 6-month review checklist |
| Required log fields | Timestamps, user IDs, access events, errors | GitHub Actions provides this | Covered |
| Monitor API calls | Watch for unexpected request rates | Not implemented | Check SP-API usage monthly in developer portal |
| Monitor for data exfiltration | Dark web / anomaly detection | N/A — small private developer | Scope: monitor GitHub for secret leaks |

#### Vulnerability Management

| Requirement | Frequency | Our Status | Action |
|---|---|---|---|
| Vulnerability scans | Monthly minimum | Not implemented | Use GitHub Security Alerts — review monthly |
| Penetration testing | Annual | N/A — no external-facing app | Scope: private developer, SP-API is not a public service |
| Code scanning | Before each release | GitHub Secret Scanning active | Extend to dependency audit |
| Critical vuln fix | 7 days | Not tracked | Track via GitHub Security tab |
| High-risk vuln fix | 30 days | Not tracked | Track via GitHub Security tab |

#### Incident Response

| Requirement | Standard | Our Status |
|---|---|---|
| Incident response plan | Must exist, reviewed every 6 months | Done — incident_response_plan.md |
| Amazon notification | Within 24 hours to security@amazon.com | Documented in plan |
| Incident Management POC | Must be designated and available | Jaiswal — rumeein@gmail.com |
| Next plan review | Dec 2026 | Scheduled |

---

### 15.6 Operational Calendar — What We Must Do and When

This is the master checklist. Run through this at every 6-month plan review (June + December).

#### Monthly
- [ ] Check GitHub Security Alerts — any exposed secrets or dependency vulnerabilities
- [ ] Check SP-API developer portal — review API call logs for unexpected activity
- [ ] Verify Windows Defender is running and definitions are current (Windows Update)

#### Quarterly
- [ ] Review `az_monthly` data — verify no PII fields have crept in
- [ ] Check GitHub Actions run logs — any failures or unexpected patterns

#### Annually (June + December review)
- [ ] Rotate SP-API LWA Client Secret (in developer portal → Rumee Dashboard app)
- [ ] Rotate Firebase API key (Google Cloud Console)
- [ ] Rotate GitHub Personal Access Token (if any)
- [ ] Rotate GROQ_API_KEY
- [ ] Verify rumee_secrets.py is NOT in any commit (`git log -S "FIREBASE_API_KEY" --all`)
- [ ] Verify Windows Defender is active
- [ ] Verify BitLocker (disk encryption) is on
- [ ] Verify no Amazon data shared with any third party
- [ ] Review incident_response_plan.md — update if anything changed
- [ ] Verify `az_monthly` data older than 18 months is purged
- [ ] Check that MFA is enabled on: Amazon Seller Central, Amazon developer portal, GitHub, Firebase Console

---

### 15.7 PII Decision Tree — Before Touching Any Order Data

Before writing any code that calls an SP-API operation:

```
Does this API call return buyer name, address, phone, or email?
│
├── YES → STOP. Requirements:
│         1. Obtain RDT via createRestrictedDataToken
│         2. Use RDT in x-amz-access-token header (not LWA token)
│         3. Do NOT store this data in any CSV, Firestore, or GitHub file
│         4. If you must store temporarily: AES-128+ encryption, 30-day deletion
│         5. Log that PII was accessed (timestamp, purpose)
│
└── NO → Standard LWA token is fine.
         Store in az_monthly / GitHub CSVs is fine.
         18-month retention cap applies.
```

---

### 15.8 What "No Third-Party Sharing" Means in Practice

| System | Does it receive Amazon data? | Verdict |
|---|---|---|
| GitHub repo (rumee-dashboard) | Yes — az_monthly aggregated non-PII | OK — hosting, not third-party sharing |
| GitHub Pages (index.html) | Yes — served to browser | OK — it's our own dashboard |
| Firebase Firestore | No Amazon data | OK |
| Groq (Vantage) | Only if we pass az_monthly to context | Verify: aggregated non-PII data is OK; never pass PII to Groq |
| Discord (Vantage bot) | Only aggregated summary stats | OK — no PII, no individual order data |
| Google Drive | No Amazon data flows here | OK |
| Any analytics tool | Never | Prohibited |
| Any competitor | Never | Prohibited |

---

### 15.9 Security Gaps — Known Issues (Update as resolved)

| Gap | Risk | Action | Priority |
|---|---|---|---|
| MFA status on Amazon accounts not confirmed | High — credential theft | Enable TOTP on Seller Central + developer portal | **Immediate** |
| BitLocker status on local machine unknown | Medium — data at rest | Verify: Settings → Privacy & Security → Device Encryption | This session |
| API key rotation not scheduled | Medium — stale credentials | Add to December 2026 review | December 2026 |
| Log review cadence not established | Medium | Add to monthly checklist | Next review |
| az_monthly 18-month purge not implemented | Low (no data yet) | Add to process.py when az_monthly has data | When SP-API live |
| GitHub Actions log retention not verified | Low | Check GitHub → Settings → Actions | Next review |
| Security monitoring system not built | Medium | Separate session — automated checks | Active item #9 in memory |

---

### 15.10 Reference Links (All Official)

| Document | URL |
|---|---|
| Policies & Agreements index | https://developer-docs.amazon.com/sp-api/docs/policies-and-agreements |
| Security & Compliance Overview | https://developer-docs.amazon.com/sp-api/docs/security-compliance-overview |
| Key Security Control Guidance | https://developer-docs.amazon.com/sp-api/docs/guidance-to-address-key-security-controls-in-sp-api-integration |
| Network Protection Guidance | https://developer-docs.amazon.com/sp-api/docs/guidance-for-network-protection-in-sp-api |
| Data Encryption & Recovery | https://developer-docs.amazon.com/sp-api/docs/protecting-amazon-api-applications-data-encryption-and-recovery |
| Restricted Data Token Guide | https://developer-docs.amazon.com/sp-api/docs/authorization-with-the-restricted-data-token |
| SP-API Guard (compliance scanner) | https://developer.amazonservices.com/guard |
| Policy changelog (Nov 2025) | https://developer-docs.amazon.com/sp-api/changelog/updates-to-the-data-protection-policy-and-acceptable-use-policy |

---

## 14. Key Decisions

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
| Secrets management | All secrets in gitignored `rumee_secrets.py` — never hardcoded in committed files. Pattern: `from rumee_secrets import SECRET_NAME`. Firebase web API key in index.html is public by design (dismiss GitHub alert). | 2026-06-22 |
| Flipkart API | Secret key stored in `rumee_secrets.py` as `FLIPKART_API_SECRET`. Also in auto-sync `secrets.js` for the `fk-api-test` tool. FK API integration in `process.py` is pending — import pattern is ready. | 2026-06-22 |
