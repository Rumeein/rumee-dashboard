# Rumee Growth System — Architecture

Last updated: 2026-06-20

---

## Three Products

| Product | Repo | Purpose |
|---|---|---|
| Chrome Extension (auto-sync) | rumee-auto-sync | Captures raw data from seller panels → uploads to Google Drive |
| Dashboard | rumee-dashboard | Processes Drive data → clean DB → visual dashboard |
| Vantage | vantage-agent (generic) + rumee-dashboard/vantage/ (instance) | AI growth advisor — reads DB, suggests experiments, tracks learnings |

All three are reusable. Any seller on Flipkart / Meesho / Amazon can plug in their own data.

---

## Platforms Covered

- Flipkart
- Meesho
- Amazon

---

## Data Flow (agreed design)

```
[Seller Panel: Flipkart / Meesho / Amazon]
        ↓  (Chrome Extension captures automatically)
[Google Drive]  ← raw files, one per day per data type
        ↓  (process.py reads via Drive API)
[GitHub repo: rumee-dashboard]  ← rumee_db_*.csv committed after every run
        ↓
[GitHub Pages: Dashboard]  ← auto-updates on every commit
        ↓
[Vantage]  ← reads DB CSVs from repo, writes experiments/learnings back to repo
```

---

## Where Each Component Runs

| Component | Runs on | Requires PC? |
|---|---|---|
| Chrome Extension | Browser (always) | Yes — but only while capturing |
| Pipeline (process.py) | GitHub Actions (nightly, scheduled) | No |
| Dashboard | GitHub Pages | No |
| Vantage nightly analysis | GitHub Actions (nightly, scheduled) | No |
| Vantage Discord Q&A (live) | Cloud server (Railway/Render) OR not built | Only if live Q&A needed |

---

## Vantage — Two Modes

### Mode 1: Nightly only (no server needed)
- GitHub Actions runs `agent.py --full-audit` every night
- Results written to `vantage/memory/experiments.json` and `learnings.json` in repo
- You read insights from the dashboard (Deep Dive tab — not built yet)
- **Cost: Free**

### Mode 2: Live Discord Q&A (server needed)
- Small cloud server runs Discord bot 24/7
- You ask questions anytime → Vantage reads repo data → instant answer
- **Cost: Free tier on Railway/Render (limited hours) or ~$5/month**

---

## LLM

- Provider: Groq (free, no credit card)
- Model: llama-3.3-70b-versatile
- API key stored in `.env` (gitignored, never in repo)

---

## Google Drive Authentication

Current: OAuth2 (requires browser login — works on PC only)
Needed for GitHub Actions: Service Account (JSON key stored as GitHub Secret — headless, no browser)
Status: NOT YET DONE — one-time setup needed before pipeline moves to GitHub Actions

---

## Build Status

| Step | Status |
|---|---|
| Chrome Extension — Flipkart + Meesho data capture | Done |
| Chrome Extension — Amazon | Not started |
| Pipeline — reads Drive API | Done |
| Pipeline — Flipkart + Meesho handlers | Done |
| Pipeline — Amazon handler | Not started |
| Pipeline on GitHub Actions (service account auth) | Not started |
| Dashboard — core metrics | Done |
| Dashboard — Deep Dive tab (experiment board) | Not started |
| Vantage — runner + context builder + LLM wiring | Done |
| Vantage — first run | Done (2026-06-20) |
| Vantage — Discord bot | Not started |
| Vantage on GitHub Actions (nightly) | Not started |
| Vantage — live Discord Q&A on cloud server | Decide after Discord bot built |

---

## Key Decisions Made

| Decision | What was decided |
|---|---|
| DB storage | DB CSVs committed to GitHub repo — GitHub is the cloud storage for processed data |
| Raw data storage | Google Drive — extension uploads directly |
| Pipeline trigger | GitHub Actions on schedule — no PC needed |
| LLM for Vantage | Groq (free) — llama-3.3-70b-versatile |
| Vantage 24/7 server | Decided later — nightly GitHub Actions is sufficient to start |
| Reusability | All three products built generically — any seller can plug in |
