"""
Rumee Dashboard — Firestore Connector
Replaces supabase_connector.py. Handles all read/write operations for
rumee_insights, rumee_tasks, and rumee_db (CSV data store) collections.

Auth: FIREBASE_CREDENTIALS env var (JSON string of service account key).
"""

import json
import os
from datetime import date, datetime, timezone

import firebase_admin
from firebase_admin import credentials, firestore


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
        db.collection('rumee_db').document(doc_id).set({
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
        ref = db.collection('rumee_insights').document()
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
            db.collection('rumee_insights')
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
        get_db().collection('rumee_insights').document(insight_id).update({'status': 'resolved'})
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
        ref = db.collection('rumee_tasks').document()
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
        get_db().collection('rumee_tasks').document(task_id).update(data)
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
            db.collection('rumee_tasks')
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
            insight_doc = db.collection('rumee_insights').document(insight_id).get()
            if insight_doc.exists:
                t['rumee_insights'] = {'id': insight_doc.id, **insight_doc.to_dict()}
            tasks.append(t)
        return tasks
    except Exception as e:
        print(f"Warning: get_completed_tasks_with_insights failed: {e}")
        return []


def write_product_master_ids(fsn_map, catalog_entries):
    """
    Write FSN (FK) and embedded-listings structure (Meesho) to product_master.

    fsn_map:        {sku_id: fsn_string}
                    — FK: patches only 'fsn' field on existing docs

    catalog_entries: {sku_id: {design, variation_type, platform, listings:[...]}}
                    — Me: full replace of doc, preserving user-entered fk_url/notes/fsn
                      and per-listing 'inactive' status (never auto-reverted by pipeline).
    """
    import re
    if not fsn_map and not catalog_entries:
        return
    try:
        db    = get_db()
        batch = db.batch()
        count = 0

        def flush(b, c):
            if c % 450 == 0 and c > 0:
                b.commit()
                return db.batch()
            return b

        # FK: only patch fsn field
        for sku_id, fsn in (fsn_map or {}).items():
            if not fsn:
                continue
            doc_id = re.sub(r'[/. ]', '_', sku_id)
            ref = db.collection('product_master').document(doc_id)
            batch.set(ref, {'fsn': str(fsn)}, merge=True)
            count += 1
            batch = flush(batch, count)

        if catalog_entries:
            # Read all existing docs once to preserve user-entered data
            existing = {}
            for snap in db.collection('product_master').get():
                existing[snap.id] = snap.to_dict() or {}

            for sku_id, entry in catalog_entries.items():
                if not entry.get('listings'):
                    continue
                doc_id = re.sub(r'[/. ]', '_', sku_id)
                old = existing.get(doc_id, {})

                # Preserve user-edited variation-level fields
                preserved = {}
                for field in ('fk_url', 'notes', 'fk_catalog_id', 'fsn'):
                    val = old.get(field)
                    if val:
                        preserved[field] = val

                # Per-listing: preserve 'inactive' status (never overwritten by pipeline)
                old_listings = old.get('listings', [])
                inactive_cats = {
                    l['catalog_id']
                    for l in old_listings
                    if isinstance(l, dict) and l.get('status') == 'inactive'
                }

                new_listings = []
                for lst in entry['listings']:
                    cat_id = lst['catalog_id']
                    if cat_id in inactive_cats:
                        status = 'inactive'
                    elif lst.get('stock', 0) > 0:
                        status = 'active'
                    else:
                        status = 'needs_review'
                    new_listings.append({
                        'style_id':   lst['style_id'],
                        'catalog_id': cat_id,
                        'product_id': lst.get('product_id', ''),
                        'me_url':     lst.get('me_url', ''),
                        'stock':      lst.get('stock', 0),
                        'status':     status,
                    })

                payload = {
                    'sku_id':         sku_id,
                    'platform':       'me',
                    'design':         entry.get('design', sku_id),
                    'variation_type': entry.get('variation_type', 'bahubali'),
                    'listings':       new_listings,
                    **preserved,
                }
                ref = db.collection('product_master').document(doc_id)
                batch.set(ref, payload)   # full replace — listings array must be rebuilt each run
                count += 1
                batch = flush(batch, count)

        if count % 450:
            batch.commit()
        print(f"  product_master: wrote {count} docs with embedded listings")
    except Exception as e:
        print(f"Warning: write_product_master_ids failed: {e}")

