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

        # Same ownership-wins fix as write_product_master_ids (see the long
        # comment there): a listing already living on a different doc than
        # what this catalog implies keeps its current home instead of being
        # resurrected at the old location.
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
            doc_meta.setdefault(pm_doc_id, {
                'design': entry.get('design', sku_id),
                'variation_type': entry.get('variation_type', 'base'),
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

