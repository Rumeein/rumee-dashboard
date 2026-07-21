"""
Rumee Dashboard — Firestore Connector
Replaces supabase_connector.py. Handles all read/write operations for
rumee_insights, rumee_tasks, and rumee_db (CSV data store) collections.

Auth: FIREBASE_CREDENTIALS env var (JSON string of service account key).
"""

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore

# Load tenant_id from tenant_config.json (same directory as this file)
_cfg_path = Path(__file__).parent / "tenant_config.json"
with open(_cfg_path, encoding="utf-8") as _f:
    _TENANT_ID = json.load(_f)["tenant_id"]

def _col(name):
    """Return tenant-prefixed collection name."""
    return f"{_TENANT_ID}_{name}"


def get_db():
    if not firebase_admin._apps:
        cred_json = os.environ.get('FIREBASE_CREDENTIALS')
        if not cred_json:
            raise ValueError("FIREBASE_CREDENTIALS env var not set")
        cred = credentials.Certificate(json.loads(cred_json))
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ── CSV data store ────────────────────────────────────────────────────────────

def write_csv_content(doc_id, csv_content):
    """Write a CSV string to rumee_db/{doc_id} (used for summary + alltime)."""
    try:
        db = get_db()
        db.collection(_col('db')).document(doc_id).set({
            'content':    csv_content,
            'updated_at': datetime.now(timezone.utc).isoformat(),
        })
        print(f"  Firestore rumee_db/{doc_id}: written ({len(csv_content):,} chars)")
    except Exception as e:
        print(f"Warning: could not write rumee_db/{doc_id} to Firestore: {e}")


def write_return_lookup(by_order_id, by_awb, window_days, freshness=None):
    """Write the Returns Scanner's live Order-ID/AWB -> SKU lookup (dashboard
    memory active.md item #72, 2026-07-21). Single doc, fully overwritten
    each run -- see process.py's call site for how by_order_id/by_awb are
    built and windowed.

    Each by_order_id entry's 'source' field is 'return' (resolved from that
    platform's own return report -- more authoritative, no age limit) or
    'order' (fallback: Orders-file data only, capped at window_days, used
    only when this specific order's return hasn't synced yet). by_awb
    resolves from Returns-file data regardless of which source won for the
    sku itself, so an AWB scanned before its return report lands will
    legitimately miss here -- an expected data-timing gap, not a bug.

    freshness: optional {platform: {last_synced, fresh}} -- lets the
    Returns Scanner show, on its initial screen, whether each platform's
    returns data is current through the last 7 days (Jaiswal, 2026-07-21),
    so staff understand why a given scan resolved via the returns-priority
    path vs. the orders-fallback path.
    """
    try:
        db = get_db()
        db.collection(_col('order_sku_lookup')).document('current').set({
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'window_days':  window_days,
            'by_order_id':  by_order_id,
            'by_awb':       by_awb,
            'freshness':    freshness or {},
        })
        print(f"  Firestore order_sku_lookup: {len(by_order_id)} orders, {len(by_awb)} AWB links (last {window_days}d)")
    except Exception as e:
        print(f"Warning: could not write order_sku_lookup: {e}")


def write_monthly_table(collection, month_key, csv_content):
    """Write one month's CSV rows to {collection}/{month_key}.
    Historical months are written once and never change; only the current
    month doc is overwritten on each daily pipeline run.
    """
    try:
        db = get_db()
        db.collection(collection).document(month_key).set({
            'content':    csv_content,
            'month':      month_key,
            'updated_at': datetime.now(timezone.utc).isoformat(),
        })
        print(f"  Firestore {collection}/{month_key}: written ({len(csv_content):,} chars)")
    except Exception as e:
        print(f"Warning: could not write {collection}/{month_key}: {e}")


# ── Packaging cost (dashboard memory active.md #46, 2026-07-12) ────────────────

def _material_cost(materials, *names):
    """First matching material's current_avg_cost by name (case-insensitive
    exact match), or 0 if none of the given names exist yet."""
    names_lower = [n.lower() for n in names]
    for m in materials:
        if (m.get('name') or '').strip().lower() in names_lower:
            return float(m.get('current_avg_cost', 0) or 0)
    return 0.0


def fetch_packaging_costs():
    """
    Real packaging/chain loss cost components from rumee_materials, replacing
    the old flat packaging_cost_per_order guess. Matches by material NAME,
    not a per-SKU BOM walk -- packaging is uniform across almost all
    products (Jaiswal, 2026-07-12; the one exception, Corrugated Box instead
    of Keeper 33 Box for combo1/2/3 and Coin Pearl Choker until that stock
    runs out, is handled by checking both names and using whichever exists).
    Missing materials contribute 0, not an error -- this returns all-zero
    until Jaiswal has actually created these materials in the Materials tab,
    which is expected, not a bug.

    Returns {'always_lost_cost', 'box_sticker_cost', 'chain_cost'}:
      always_lost_cost -- Label + Branded Poly + Brand Card + Transparent
                           Poly, always lost on any return (Jaiswal's rule).
      box_sticker_cost -- Keeper 33 Box (or Corrugated Box) + Rumee Sticker,
                           lost together only when box condition = Damaged.
      chain_cost        -- first active material with "chain" in its name;
                           lost only when chain condition = Damaged. Best-
                           effort (not per-design) until real per-product
                           BOM data justifies a more precise lookup.
    """
    try:
        db = get_db()
        docs = db.collection(_col('materials')).where('status', '==', 'active').stream()
        materials = [d.to_dict() for d in docs]
    except Exception as e:
        print(f"Warning: could not fetch materials for packaging cost: {e}")
        return {'always_lost_cost': 0.0, 'box_sticker_cost': 0.0, 'chain_cost': 0.0}

    always_lost = (
        _material_cost(materials, 'Label')
        + _material_cost(materials, 'Branded Poly')
        + _material_cost(materials, 'Brand Card')
        + _material_cost(materials, 'Transparent Poly')
    )
    box_sticker = (
        _material_cost(materials, 'Keeper 33 Box', 'Corrugated Box')
        + _material_cost(materials, 'Rumee Sticker')
    )
    chain_cost = 0.0
    for m in materials:
        if 'chain' in (m.get('name') or '').lower():
            chain_cost = float(m.get('current_avg_cost', 0) or 0)
            break

    return {'always_lost_cost': always_lost, 'box_sticker_cost': box_sticker, 'chain_cost': chain_cost}


# ── Users / roles (dashboard memory active.md #41, 2026-07-11) ─────────────────

def write_user(email, role, added_by='process.py --seed-users'):
    """Create/update a rumee_users/{email} doc via the Admin SDK -- bypasses
    firestore.rules by design, since this is the ONLY way to bootstrap the
    first owner record (the rules' isOwner()/isStaffOrOwner() checks depend
    on this collection existing, so nobody could pass them to write it
    themselves before it exists). Ongoing staff add/remove after the first
    owner record happens through the dashboard's own Manage Users screen
    (client-side, owner-authenticated), not this function.
    """
    try:
        db = get_db()
        db.collection(_col('users')).document(email).set({
            'email':      email,
            'role':       role,
            'added_by':   added_by,
            'added_at':   datetime.now(timezone.utc).isoformat(),
        }, merge=True)
        print(f"  Firestore {_col('users')}/{email}: role={role}")
    except Exception as e:
        print(f"Warning: could not write user {email}: {e}")


# ── Insights ──────────────────────────────────────────────────────────────────

def write_insight(platform, sku_id, sku_name, category, text, severity='info'):
    """Write a new insight. Returns the inserted doc as dict with 'id'."""
    try:
        db  = get_db()
        ref = db.collection(_col('insights')).document()
        data = {
            'platform':        platform,
            'sku_id':          sku_id,
            'sku_name':        sku_name,
            'category':        category,
            'insight_text':    text,
            'severity':        severity,
            'status':          'new',
            'email_count':     0,
            'last_emailed_date': None,
            'created_at':      datetime.now(timezone.utc).isoformat(),
        }
        ref.set(data)
        return {'id': ref.id, **data}
    except Exception as e:
        print(f"Warning: could not write insight: {e}")
        return None


def insight_exists_today(sku_id, category):
    """Return True if an unresolved insight for this SKU+category was written today."""
    try:
        db    = get_db()
        today = date.today().isoformat()
        docs  = (
            db.collection(_col('insights'))
            .where('sku_id',   '==', sku_id)
            .where('category', '==', category)
            .where('status',   '!=', 'resolved')
            .stream()
        )
        return any((d.to_dict().get('created_at', '') or '')[:10] == today for d in docs)
    except Exception:
        return False


def mark_insight_resolved(insight_id):
    """Mark a single insight as resolved."""
    try:
        get_db().collection(_col('insights')).document(insight_id).update({'status': 'resolved'})
        return True
    except Exception as e:
        print(f"Warning: could not resolve insight {insight_id}: {e}")
        return False


# ── Notification Center (active.md item #70, 2026-07-20) ───────────────────────
# A new, separate feed from rumee_insights/the Data Pipeline Map (Jaiswal's
# explicit call, 2026-07-20) -- every error/warning gathered during a
# process.py run (business-data AND technical/infra) gets synced here so it
# surfaces in the dashboard's bell icon, not just the local
# pipeline_run_log.json / GitHub Actions console output nobody checks daily.

_NOTIF_DEFAULT_IMPACT = {
    'FK': "may affect Flipkart data accuracy for this run",
    'ME': "may affect Meesho data accuracy for this run",
    'AMAZON': "may affect Amazon data accuracy for this run",
    'CATALOG': "may affect Products/catalog mapping accuracy for this run",
    'STOCK': "may affect stock/inventory accuracy for this run",
    'INFRA': "infrastructure/technical issue — check details",
}

def sync_pipeline_notifications(run_errors, run_warnings, run_id):
    """
    Pushes every entry in run_errors (severity='critical')/run_warnings
    (severity='warning') to rumee_notifications, tagged source='pipeline'.
    Dedupes against an already-OPEN pipeline notification for the same
    (category, file) by bumping it in place instead of creating a new doc
    each run — a persisting daily issue on the same file/stream shouldn't
    flood the center with near-identical entries. `created_at` is set once
    at first creation and never touched again on a bump, so a chronic
    multi-day failure still shows its real age instead of looking like it
    just started; `last_seen` updates on every bump instead.

    Deliberately does NOT auto-resolve anything (independent code review
    finding, 2026-07-20: an earlier version resolved any open notification
    whose category was simply absent from a given run's errors/warnings —
    but several streams don't run every invocation, e.g. AZ Catalog's
    weekly pull, so "category didn't fire this run" is not the same as
    "the issue is fixed," and that version could silently mark a genuinely
    still-broken issue as resolved — exactly the class of problem this
    feature exists to prevent). Pipeline notifications are only ever
    resolved by a human clicking "Mark Resolved," same as app notifications.
    Best-effort: swallows its own failures (caller already wraps this in a
    try/except too) so a notification-sync problem never blocks the pipeline.
    """
    try:
        db = get_db()
        col = db.collection(_col('notifications'))
        now_iso = datetime.now(timezone.utc).isoformat()

        entries = ([dict(e, severity='critical') for e in (run_errors or [])] +
                   [dict(e, severity='warning') for e in (run_warnings or [])])

        open_docs = list(col.where('source', '==', 'pipeline').where('status', '==', 'open').stream())
        open_by_cat_file = {(d.to_dict().get('category'), d.to_dict().get('file')): d.reference for d in open_docs}

        for e in entries:
            cat = e.get('type', 'INFRA')
            file_ = e.get('file')
            impact = e.get('impact') or _NOTIF_DEFAULT_IMPACT.get(cat, "check details")
            existing_ref = open_by_cat_file.get((cat, file_))
            if existing_ref:
                existing_ref.update({
                    'run_id': run_id,
                    'severity': e.get('severity', 'warning'),
                    'message': e.get('reason', ''),
                    'impact': impact,
                    'last_seen': now_iso,
                })
            else:
                col.document().set({
                    'source': 'pipeline',
                    'run_id': run_id,
                    'category': cat,
                    'severity': e.get('severity', 'warning'),
                    'message': e.get('reason', ''),
                    'impact': impact,
                    'file': file_,
                    'status': 'open',
                    'created_at': now_iso,
                    'last_seen': now_iso,
                })

        print(f"  Notification Center: {len(entries)} entries synced")
    except Exception as e:
        print(f"Warning: could not sync pipeline notifications: {e}")


# ── Tasks ─────────────────────────────────────────────────────────────────────

def write_task(task_text, platform, sku_id=None, priority='medium',
               due_date=None, linked_insight_id=None, created_by='pipeline'):
    """Write a new task. Returns dict with 'id'."""
    try:
        db  = get_db()
        ref = db.collection(_col('tasks')).document()
        ref.set({
            'task_text':         task_text,
            'platform':          platform,
            'sku_id':            sku_id,
            'priority':          priority,
            'due_date':          str(due_date) if due_date else None,
            'linked_insight_id': linked_insight_id,
            'created_by':        created_by,
            'status':            'pending',
            'created_at':        datetime.now(timezone.utc).isoformat(),
        })
        return {'id': ref.id}
    except Exception as e:
        print(f"Warning: could not write task: {e}")
        return None


def mark_task_status(task_id, status):
    """Update task status. Records completion time if marking done."""
    try:
        data = {'status': status}
        if status == 'done':
            data['completed_at'] = datetime.now(timezone.utc).isoformat()
        get_db().collection(_col('tasks')).document(task_id).update(data)
        return True
    except Exception as e:
        print(f"Warning: could not update task {task_id}: {e}")
        return False


def get_completed_tasks_with_insights(cutoff_iso):
    """
    Return tasks marked done after cutoff_iso that have a linked insight.
    Each task dict includes a 'rumee_insights' key with the linked insight data.
    """
    try:
        db    = get_db()
        docs  = (
            db.collection(_col('tasks'))
            .where('status',       '==', 'done')
            .where('completed_at', '>=', cutoff_iso)
            .stream()
        )
        tasks = []
        for doc in docs:
            t = doc.to_dict()
            t['id'] = doc.id
            insight_id = t.get('linked_insight_id')
            if not insight_id:
                continue
            insight_doc = db.collection(_col('insights')).document(insight_id).get()
            if insight_doc.exists:
                t['rumee_insights'] = {'id': insight_doc.id, **insight_doc.to_dict()}
            tasks.append(t)
        return tasks
    except Exception as e:
        print(f"Warning: get_completed_tasks_with_insights failed: {e}")
        return []


def load_pm_overrides():
    """Read all pm_overrides docs once per pipeline run.
    Returns {f'{platform}_{catalog_id}': {'target_sku_id', 'target_variation_type',
             'target_design', ...}} — the full doc dict (label single source of
    truth, Option A). An empty/missing collection is a legitimate {} return.

    Does NOT swallow connection/auth failures (e.g. Firestore outage, bad
    FIREBASE_CREDENTIALS) — those propagate so the caller can tell "load
    failed" apart from "collection is genuinely empty" and skip catalog
    processing for the run instead of silently treating every SKU as
    unmapped (found 2026-07-04: a missing-credentials failure here used to
    return {} and flood needs_review with 100% of that run's rows).
    """
    db = get_db()
    out = {}
    for snap in db.collection('pm_overrides').get():
        d = snap.to_dict() or {}
        if d.get('target_sku_id'):
            out[snap.id] = d
    return out


def write_needs_review(entries):
    """Upsert needs_review docs for unmapped listings.
    entries: list of dicts {platform, catalog_id, raw_sku, product_name}
    Doc id: nr_{platform}_{catalog_id}. Idempotent — safe to call repeatedly
    for the same listing across pipeline runs.
    """
    if not entries:
        return
    try:
        db = get_db()
        batch = db.batch()
        count = 0
        for e in entries:
            if not e.get('catalog_id'):
                continue
            doc_id = f"nr_{e['platform']}_{e['catalog_id']}"
            ref = db.collection('needs_review').document(doc_id)
            batch.set(ref, {
                'platform':     e['platform'],
                'catalog_id':   e['catalog_id'],
                'raw_sku':      e.get('raw_sku', ''),
                'product_name': e.get('product_name', ''),
                'reason':       e.get('reason', 'Not in your saved SKU list yet'),
                'status':       'needs_review',
            }, merge=True)
            count += 1
            if count % 450 == 0:
                batch.commit()
                batch = db.batch()
        if count % 450:
            batch.commit()
        print(f"  needs_review: upserted {count} docs")
    except Exception as e:
        print(f"Warning: write_needs_review failed: {e}")


def write_product_master_ids(catalog_entries):
    """
    Write the embedded-listings structure to product_master, keyed by the
    LABEL folder (target_sku_id) — the single source of truth (Option A,
    2026-07-03). Platform-generic: Meesho, Flipkart, Shopsy and Amazon
    listings all flow through here (doc id = label folder, e.g. "DJ-7 Bahubali").

    Pipeline write discipline (mandatory — root cause of a prior data-loss
    incident was unscoped writes): NEVER touch 'notes' or 'fk_url' (dashboard-
    owned). NEVER change doc-level 'status' (dashboard-owned; pipeline only
    sets it to 'active' on first-ever doc creation). NEVER full-doc .set()
    overwrite an existing doc — merge=True always, and listings[] is merged
    by catalog_id (existing entries updated in place, new ones appended),
    never wholesale replaced. FSN lives inside each listing entry (fsn field),
    never as a bare top-level write onto a slug doc (that created orphan docs).

    catalog_entries: {label_folder: {design, variation_type, platform, listings:[...]}}
                     Each listing dict may carry (all optional-with-fallback):
                     sku_id/style_id, catalog_id, product_id, me_url/buyer_url,
                     fsn, stock, platform, suggested_inactive.
    """
    import re
    from datetime import datetime, timezone
    if not catalog_entries:
        return
    try:
        db    = get_db()
        batch = db.batch()
        count = 0
        now   = datetime.now(timezone.utc).isoformat()

        def flush(b, c):
            if c % 450 == 0 and c > 0:
                b.commit()
                return db.batch()
            return b

        if catalog_entries:
            existing = {}
            for snap in db.collection('product_master').get():
                existing[snap.id] = snap.to_dict() or {}

            # sku_id (the doc id) comes from an arbitrary label folder chosen
            # at Assign time, not derived from (design, variation_type) — so
            # two different runs/actions can independently pick two different
            # sku_ids for what is really the same design+variation, creating
            # duplicate docs. label_index lets a new write redirect into the
            # doc that already owns that (design, variation_type) pair.
            #
            # Normalize via the SAME folder-name collapsing rule as
            # index.html's _pmFolderName() (Base/empty variation -> design
            # only; design==variation -> design only) — not just a raw
            # (design, variation_type) tuple. A raw tuple would treat
            # ("Bangle-4", "Base") and ("Bangle-4", "Bangle-4") as different
            # keys even though _pmFolderName collapses both to the doc id
            # "Bangle-4", missing exactly the duplicates this guard exists
            # to catch.
            def _pm_folder_name(design, variation):
                l1 = str(design or '').strip()
                l2 = str(variation or '').strip()
                if not l2 or l2.lower() == 'base':
                    return l1
                if l1.lower() == l2.lower():
                    return l1
                return (l1 + ' ' + l2).strip()

            def _norm_label(design, vtype):
                return _pm_folder_name(design, vtype).lower()

            label_index = {
                _norm_label(d.get('design'), d.get('variation_type')): doc_id
                for doc_id, d in existing.items()
            }

            # Merge key = product_id when present (Meesho, per-product), else
            # catalog_id/FSN/asin (FK/Shopsy/Amazon). Keeps one entry per real
            # listing; Meesho catalogs holding several products don't collide.
            def _mkey(l):
                return str(l.get('product_id') or l.get('catalog_id') or '')

            # Listing-ownership index: where does each real listing already
            # live RIGHT NOW, as of this run's snapshot? pm_overrides records
            # a one-time assignment decision and is immutable once written
            # (Firestore rules: allow update/delete: if false — a deliberate
            # 2026-07-02 security hardening, public client can never rewrite
            # it). Rename/Reassign in the dashboard correctly move a listing's
            # doc in product_master, but have no way to also update
            # pm_overrides. Without this index, the next pipeline run would
            # trust pm_overrides' stale target and silently resurrect the
            # listing back at its old location — duplicating it alongside
            # wherever it was manually moved to (confirmed live 2026-07-09:
            # Combo sub-type listings kept reappearing in the old flat
            # "Combo" bucket after being reassigned into "Combo_OC" etc).
            # Fix: current product_master placement always wins over
            # pm_overrides when the two disagree. pm_overrides still decides
            # placement for any listing that has never appeared in
            # product_master before (mkey not in this index).
            listing_owner = {}
            for doc_id, d in existing.items():
                for l in d.get('listings', []):
                    if isinstance(l, dict):
                        k = _mkey(l)
                        if k:
                            listing_owner[k] = doc_id

            # Group every incoming listing update by its REAL target doc
            # (existing owner if one exists, else the pm_overrides-implied
            # folder) before writing anything — a listing already owned
            # elsewhere must never be added to the pm_overrides-implied doc.
            from collections import defaultdict
            doc_updates = defaultdict(dict)   # doc_id -> {mkey: new_entry}
            doc_meta    = {}                  # doc_id -> {design, variation_type} (new docs only)

            for sku_id, entry in catalog_entries.items():
                if not entry.get('listings'):
                    continue
                pm_doc_id = re.sub(r'[/. ]', '_', sku_id)
                design = entry.get('design', sku_id)
                variation_type = entry.get('variation_type', 'Base')

                if pm_doc_id not in existing:
                    redirect_id = label_index.get(_norm_label(design, variation_type))
                    if redirect_id and redirect_id != pm_doc_id:
                        pm_doc_id = redirect_id
                    else:
                        # Register this brand-new doc's label immediately so a
                        # later sku_id in this SAME run that shares the same
                        # design+variation redirects here too, instead of each
                        # getting its own new doc for what's really one design
                        # (label_index is built once from `existing` above this
                        # loop, so without this it only catches duplicates
                        # across separate pipeline runs, not within one).
                        label_index[_norm_label(design, variation_type)] = pm_doc_id

                doc_meta.setdefault(pm_doc_id, {'design': design, 'variation_type': variation_type, 'sku_id': sku_id})

                _entry_plat = {'me': 'meesho'}.get(entry.get('platform'), entry.get('platform'))
                for lst in entry['listings']:
                    mkey  = _mkey(lst)                 # merge key (product_id or catalog_id)
                    stock = lst.get('stock', 0)
                    new_entry = {
                        'platform':          lst.get('platform') or _entry_plat or 'meesho',
                        'sku_id':            lst.get('sku_id') or lst.get('style_id', ''),
                        'catalog_id':        lst.get('catalog_id', ''),   # real catalog id (display)
                        'stock':             stock,
                        'listing_quality':   None,
                        'buyer_url':         lst.get('buyer_url') or lst.get('me_url', ''),
                        'low_stock_alert':   stock == 0,
                        'suggested_inactive': lst.get('suggested_inactive', False),
                        'updated_at':        now,
                    }
                    if lst.get('product_id'):
                        new_entry['product_id'] = str(lst['product_id'])
                    if lst.get('fsn'):
                        new_entry['fsn'] = str(lst['fsn'])

                    owner = listing_owner.get(mkey)
                    target_doc_id = owner if (owner and owner != pm_doc_id) else pm_doc_id
                    doc_updates[target_doc_id][mkey] = new_entry
                    # Keep the index current as decisions are made this run —
                    # without this, the same listing appearing under two
                    # different sku_id groups within ONE run (neither with a
                    # pre-existing owner) could independently resolve to two
                    # different target docs, duplicating it. Found 2026-07-10
                    # code review — label_index already did this (line ~380),
                    # listing_owner didn't.
                    listing_owner[mkey] = target_doc_id

            for doc_id, updates in doc_updates.items():
                old = existing.get(doc_id, {})
                is_new_doc = doc_id not in existing

                old_listings = {
                    _mkey(l): l for l in old.get('listings', [])
                    if isinstance(l, dict) and _mkey(l)
                }
                merged_map = dict(old_listings)
                merged_map.update(updates)
                merged_listings = list(merged_map.values())

                meta = doc_meta.get(doc_id, {})
                # Existing doc keeps its OWN design/variation_type — it may
                # differ from what pm_overrides implies for a listing that
                # got redirected here (that's the whole point: this doc's
                # identity, set by a human via Rename/Reassign, wins).
                design         = old.get('design') or meta.get('design') or doc_id
                variation_type = old.get('variation_type') or meta.get('variation_type') or 'Base'

                # Redirected onto an existing doc under a different sku_id —
                # keep that doc's own sku_id field, don't overwrite its
                # identity with the freshly-computed one. Fall back to doc_id
                # itself (the real Firestore key), not the current loop's
                # un-redirected sku_id, if the existing doc predates this
                # schema and has no sku_id field of its own — using sku_id
                # here would write a value that doesn't match the actual doc.
                doc_sku_id = (old.get('sku_id') or doc_id) if not is_new_doc else meta.get('sku_id', doc_id)

                payload = {
                    'sku_id':         doc_sku_id,
                    'design':         design,
                    'variation_type': variation_type,
                    'listings':       merged_listings,
                }
                if is_new_doc:
                    payload['status']     = 'active'
                    payload['created_at'] = now
                # 'notes' and 'fk_url' deliberately never written here — dashboard-owned.

                ref = db.collection('product_master').document(doc_id)
                batch.set(ref, payload, merge=True)
                count += 1
                batch = flush(batch, count)

        if count % 450:
            batch.commit()
        print(f"  product_master: wrote {count} docs (merge=True, targeted fields only)")
    except Exception as e:
        print(f"Warning: write_product_master_ids failed: {e}")


def write_az_product_master(listings_by_sku):
    """
    Merge Amazon catalog listings into product_master. Same write discipline
    as write_product_master_ids: merge=True, never touch notes/fk_url/status
    on existing docs, set status=active + created_at only on first creation,
    merge listings[] by catalog_id (asin/listing-id).

    listings_by_sku: {sku_id: {design, variation_type, listings:[...]}}
        each listing: {sku_id, catalog_id, stock, buyer_url, low_stock_alert, suggested_inactive}
    """
    import re
    from datetime import datetime, timezone
    if not listings_by_sku:
        return
    try:
        db    = get_db()
        batch = db.batch()
        count = 0
        now   = datetime.now(timezone.utc).isoformat()

        existing = {}
        for snap in db.collection('product_master').get():
            existing[snap.id] = snap.to_dict() or {}

        # Same label_index redirect as write_product_master_ids (see the long
        # comment there) — without this, an Amazon sku_id that normalizes to
        # a (design, variation_type) already owned by a different doc id
        # would silently create a second, duplicate doc instead of merging
        # into the existing one. Found missing 2026-07-10 code review: this
        # function had the ownership-wins fix below but not this one.
        def _pm_folder_name(design, variation):
            l1 = str(design or '').strip()
            l2 = str(variation or '').strip()
            if not l2 or l2.lower() == 'base':
                return l1
            if l1.lower() == l2.lower():
                return l1
            return (l1 + ' ' + l2).strip()

        def _norm_label(design, vtype):
            return _pm_folder_name(design, vtype).lower()

        label_index = {
            _norm_label(d.get('design'), d.get('variation_type')): doc_id
            for doc_id, d in existing.items()
        }

        # Same ownership-wins fix as write_product_master_ids (see the long
        # comment there): a listing already living on a different doc than
        # what this catalog implies keeps its current home instead of being
        # resurrected at the old location. Updated as target_doc_id is
        # decided (not just seeded from the pre-run snapshot) — otherwise the
        # same catalog_id appearing under two different sku_id groups in one
        # run could be written to two different docs (same staleness bug
        # found and fixed in write_product_master_ids's listing_owner).
        listing_owner = {}
        for doc_id, d in existing.items():
            for l in d.get('listings', []):
                if isinstance(l, dict) and l.get('catalog_id'):
                    listing_owner[l['catalog_id']] = doc_id

        from collections import defaultdict
        doc_updates = defaultdict(dict)   # doc_id -> {catalog_id: new_entry}
        doc_meta    = {}                  # doc_id -> {design, variation_type, sku_id} (new docs only)

        for sku_id, entry in listings_by_sku.items():
            if not entry.get('listings'):
                continue
            pm_doc_id = re.sub(r'[/. ]', '_', sku_id)
            design = entry.get('design', sku_id)
            variation_type = entry.get('variation_type', 'base')

            if pm_doc_id not in existing:
                redirect_id = label_index.get(_norm_label(design, variation_type))
                if redirect_id and redirect_id != pm_doc_id:
                    pm_doc_id = redirect_id
                else:
                    label_index[_norm_label(design, variation_type)] = pm_doc_id

            doc_meta.setdefault(pm_doc_id, {
                'design': design,
                'variation_type': variation_type,
                'sku_id': sku_id,
            })

            for lst in entry['listings']:
                cat_id = lst['catalog_id']
                new_entry = {
                    'platform':           'amazon',
                    'sku_id':             lst['sku_id'],
                    'catalog_id':         cat_id,
                    'stock':              lst.get('stock', 0),
                    'listing_quality':    None,
                    'buyer_url':          lst.get('buyer_url', ''),
                    'low_stock_alert':    lst.get('low_stock_alert', False),
                    'suggested_inactive': lst.get('suggested_inactive', False),
                    'updated_at':         now,
                }
                owner = listing_owner.get(cat_id)
                target_doc_id = owner if (owner and owner != pm_doc_id) else pm_doc_id
                doc_updates[target_doc_id][cat_id] = new_entry
                listing_owner[cat_id] = target_doc_id

        for doc_id, updates in doc_updates.items():
            old = existing.get(doc_id, {})
            is_new_doc = doc_id not in existing

            old_listings = {
                l['catalog_id']: l for l in old.get('listings', [])
                if isinstance(l, dict) and l.get('catalog_id')
            }
            merged_map = dict(old_listings)
            merged_map.update(updates)
            merged_listings = list(merged_map.values())

            meta = doc_meta.get(doc_id, {})
            design         = old.get('design') or meta.get('design') or doc_id
            variation_type = old.get('variation_type') or meta.get('variation_type') or 'base'
            doc_sku_id     = old.get('sku_id') or meta.get('sku_id', doc_id)

            payload = {
                'sku_id':         doc_sku_id,
                'design':         design,
                'variation_type': variation_type,
                'listings':       merged_listings,
            }
            if is_new_doc:
                payload['status']     = 'active'
                payload['created_at'] = now

            ref = db.collection('product_master').document(doc_id)
            batch.set(ref, payload, merge=True)
            count += 1
            if count % 450 == 0:
                batch.commit()
                batch = db.batch()

        if count % 450:
            batch.commit()
        print(f"  product_master (Amazon): merged {count} docs")
    except Exception as e:
        print(f"Warning: write_az_product_master failed: {e}")


# ─── Stock decrement / return credit-back (dashboard memory active.md item ───
# #64, 2026-07-17). Sale-triggered stock decrement + return credit-back:
# resolves a platform order's SKU to a real Product Master item, walks that
# item's BOM (built in the dashboard, rumee_boms/output_type=final), and
# moves Item Master stock accordingly. See process.py's own call sites for
# the resolution logic per platform (join field differs: FK/Amazon use the
# raw sku field, Meesho uses sku_name -- confirmed against real live data,
# not guessed, 2026-07-17).

def load_materials():
    """Returns {material_id: {...full doc...}} for every rumee_materials doc."""
    db = get_db()
    out = {}
    for snap in db.collection(_col('materials')).get():
        out[snap.id] = snap.to_dict() or {}
    return out


def load_product_master_variation_types():
    """Returns {product_master_doc_id: variation_type} for every live doc --
    used to tell a Bahubali order from an OG/Base one (dashboard memory
    active.md item #72, 2026-07-21) without ever guessing from the raw SKU
    string, which the standing rule in CLAUDE.md explicitly forbids
    ("variation_type is free text set by a human ... never guessed from SKU
    text"). Product Master is the one authoritative source for this."""
    db = get_db()
    out = {}
    for snap in db.collection('product_master').get():
        d = snap.to_dict() or {}
        out[snap.id] = d.get('variation_type', '')
    return out


def load_final_boms():
    """Returns {product_master_id: {...full doc...}} for every rumee_boms doc
    with output_type == 'final' (the only kind relevant to a sale -- an
    intermediate-output BOM feeds Conversion Batches, not sales)."""
    db = get_db()
    out = {}
    for snap in db.collection(_col('boms')).get():
        d = snap.to_dict() or {}
        if d.get('output_type') == 'final':
            out[snap.id] = d
    return out


def load_product_master_sku_index():
    """
    Returns {(platform, normalized_sku_id): product_master_doc_id} built
    from every listing in every live product_master doc. normalized_sku_id
    is .strip().lower() -- same case/whitespace tolerance already used
    throughout this codebase's own product_master matching (index.html's
    _pmLabelKey). Platform values match what listings already store:
    'meesho', 'flipkart', 'amazon' -- callers must normalize their own
    platform string to match before looking up.
    """
    db = get_db()
    index = {}
    for snap in db.collection('product_master').get():
        d = snap.to_dict() or {}
        for listing in (d.get('listings') or []):
            sku_id   = str(listing.get('sku_id') or '').strip().lower()
            platform = str(listing.get('platform') or '').strip().lower()
            if sku_id and platform:
                index[(platform, sku_id)] = snap.id
    return index


def load_stock_sku_overrides():
    """Returns {(platform, normalized_raw_sku): product_master_id} -- manual
    mappings for order SKUs that don't exact-match any product_master
    listing (real naming drift confirmed in live data, e.g. "Bahubali DJ7"
    vs "DJ-7 Bahubali"). Set via the dashboard's Stock tab mapping UI."""
    db = get_db()
    out = {}
    for snap in db.collection(_col('stock_sku_overrides')).get():
        d = snap.to_dict() or {}
        platform = str(d.get('platform') or '').strip().lower()
        raw_sku  = str(d.get('raw_sku') or '').strip().lower()
        target   = d.get('product_master_id')
        if platform and raw_sku and target:
            out[(platform, raw_sku)] = target
    return out


def write_stock_unresolved(entries):
    """
    Upserts entries into rumee_stock_unresolved -- one doc per
    (platform, normalized raw sku) that failed to resolve to a Product
    Master item. Increments order_count / extends last_seen on repeat
    occurrences rather than overwriting, so the dashboard's mapping UI
    shows real accumulated impact, not just "seen once."
    entries: [{platform, raw_sku, date, qty}]
    """
    if not entries:
        return
    try:
        db = get_db()
        existing = {}
        for snap in db.collection(_col('stock_unresolved')).get():
            existing[snap.id] = snap.to_dict() or {}

        now = datetime.now(timezone.utc).isoformat()
        touched = {}
        for e in entries:
            platform = str(e.get('platform') or '').strip().lower()
            raw_sku  = str(e.get('raw_sku') or '').strip()
            if not platform or not raw_sku:
                continue
            doc_id = f"{platform}_{raw_sku.lower()}".replace('/', '_').replace('.', '_')[:200]
            cur = touched.get(doc_id) or dict(existing.get(doc_id) or {
                'platform': platform, 'raw_sku': raw_sku,
                'first_seen': e.get('date') or now, 'order_count': 0, 'status': 'pending',
            })
            cur['order_count'] = int(cur.get('order_count', 0) or 0) + int(e.get('qty', 1) or 1)
            cur['last_seen']   = e.get('date') or now
            cur['updated_at']  = now
            touched[doc_id] = cur

        batch = db.batch()
        count = 0
        for doc_id, fields in touched.items():
            ref = db.collection(_col('stock_unresolved')).document(doc_id)
            batch.set(ref, fields, merge=True)
            count += 1
            if count % 450 == 0:
                batch.commit()
                batch = db.batch()
        if count % 450:
            batch.commit()
        print(f"  stock_unresolved: upserted {count} SKU(s)")
    except Exception as e:
        print(f"Warning: write_stock_unresolved failed: {e}")


def apply_stock_movements(movements):
    """
    Applies a list of stock movements to rumee_materials + posts matching
    rumee_stock_ledger entries -- the Admin-SDK/pipeline-side twin of
    index.html's postStockMovement(), same weighted-average-cost formula
    (OUT never changes avg cost, only reduces stock; IN blends by qty*cost).
    Movements for the same material_id are applied in the order given (list
    order matters -- caller should sort by date), reading the material's
    current state ONCE per material (not once per movement) for efficiency,
    but computing the running (stock, avg_cost) sequentially in memory so
    multi-movement math within one run is still correct.

    movements: [{material_id, direction: 'in'|'out', qty, unit_cost,
                 source_type, source_id, notes, date}]
    Returns: {material_id: final_stock} for whatever was actually applied --
    a movement referencing an unknown material_id is skipped with a warning,
    never crashes the whole batch over one bad id.
    """
    if not movements:
        return {}
    db = get_db()
    by_material = {}
    for m in movements:
        by_material.setdefault(m['material_id'], []).append(m)

    materials_ref = db.collection(_col('materials'))
    ledger_ref    = db.collection(_col('stock_ledger'))
    results = {}
    batch = db.batch()
    count = 0
    now = datetime.now(timezone.utc).isoformat()

    for material_id, mvts in by_material.items():
        snap = materials_ref.document(material_id).get()
        if not snap.exists:
            print(f"  Warning: apply_stock_movements skipped unknown material_id {material_id}")
            continue
        m = snap.to_dict() or {}
        stock    = float(m.get('current_stock', 0) or 0)
        avg_cost = float(m.get('current_avg_cost', 0) or 0)

        for mv in mvts:
            qty = float(mv.get('qty', 0) or 0)
            if mv['direction'] == 'in':
                unit_cost = float(mv.get('unit_cost', 0) or 0)
                new_stock = stock + qty
                avg_cost  = ((stock * avg_cost) + (qty * unit_cost)) / new_stock if new_stock > 0 else unit_cost
                stock = new_stock
            else:
                stock = stock - qty
            # Deterministic, content-derived -- NOT a positional counter
            # (an earlier version suffixed this with the batch-local `count`,
            # which happened to reset to 0 on every separate call/run; an
            # independent review caught that this meant two SEPARATE
            # pipeline runs reprocessing the "same" movement -- which
            # shouldn't happen, but would silently mask it as a single
            # ledger entry overwrite instead of surfacing it as a visible
            # duplicate). Keying purely on (material_id, source_type,
            # source_id) makes a genuine re-submission of the same movement
            # idempotent at the ledger level too, matching the same
            # never-guess/never-hide principle used everywhere else in this
            # feature.
            entry_id = f"{material_id}_{mv.get('source_type')}_{mv.get('source_id')}"
            batch.set(ledger_ref.document(entry_id), {
                'entry_id':      entry_id,
                'material_id':   material_id,
                'material_name': m.get('name'),
                'date':          mv.get('date') or now[:10],
                'direction':     mv['direction'],
                'qty':           qty,
                'unit_cost':     mv.get('unit_cost'),
                'source_type':   mv.get('source_type'),
                'source_id':     mv.get('source_id'),
                'notes':         mv.get('notes'),
                'entered_by':    'pipeline',
                'entered_at':    now,
            }, merge=True)
            count += 1
            if count % 450 == 0:
                batch.commit()
                batch = db.batch()

        batch.set(materials_ref.document(material_id), {
            'current_stock':    round(stock, 4),
            'current_avg_cost': round(avg_cost, 4),
        }, merge=True)
        results[material_id] = stock
        count += 1
        if count % 450 == 0:
            batch.commit()
            batch = db.batch()

    if count % 450:
        batch.commit()
    return results

