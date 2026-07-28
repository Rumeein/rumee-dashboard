import re
import time
import logging
import threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import firebase_admin
from firebase_admin import credentials, firestore, auth as fb_auth
from google.api_core import exceptions as gcloud_exceptions

CRED_PATH = '/opt/pm-service/credentials.json'
ALLOWED_ORIGIN = 'https://rumeein.github.io'

# Sanity backstop against a runaway BUG in a legitimate signed-in session
# (e.g. an accidental loop), not an anti-abuse measure anymore -- real
# access control is now the Firebase ID token check below (2026-07-27,
# Jaiswal: "move the PM to lockdown"). 100/hour comfortably covers a
# legitimate bulk rename across this app's whole catalog (~84 docs total).
RATE_LIMIT_MAX = 100
RATE_LIMIT_WINDOW_SEC = 3600
_rate_lock = threading.Lock()
_mutation_timestamps = []

# product_master fields /pm/restore-doc is ever allowed to set -- mirrors
# firestore.rules' own update whitelist PLUS sku_id/created_at, which the
# rules explicitly forbid touching via a normal update but which a genuine
# restore legitimately needs to set (recreating a doc's identity), same
# convention process.py's own pipeline writes already use.
ALLOWED_FIELDS = {'sku_id', 'design', 'variation_type', 'status', 'notes',
                   'fk_url', 'product_type', 'listings', 'created_at'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
log = logging.getLogger('pm_service')

cred = credentials.Certificate(CRED_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

app = Flask(__name__)


# Folder-name rule -- MUST stay identical to process.py's folder() and
# index.html's _pmFolderName(): Base/empty variation -> design only;
# design==variation -> design only; else "design variation".
def pm_folder_name(design, variation):
    l1 = (design or '').strip()
    l2 = (variation or '').strip()
    if not l2 or l2.lower() == 'base':
        return l1
    if l1.lower() == l2.lower():
        return l1
    return f'{l1} {l2}'.strip()


def pm_label_key(design, variation):
    return pm_folder_name(design, variation).lower()


def _listing_key(l):
    if not isinstance(l, dict):
        return ''
    return str(l.get('product_id') or l.get('catalog_id') or '')


def _rate_limit_ok():
    now = time.monotonic()
    with _rate_lock:
        cutoff = now - RATE_LIMIT_WINDOW_SEC
        while _mutation_timestamps and _mutation_timestamps[0] < cutoff:
            _mutation_timestamps.pop(0)
        if len(_mutation_timestamps) >= RATE_LIMIT_MAX:
            return False
        _mutation_timestamps.append(now)
        return True


def _verify_owner():
    """Returns the verified signed-in owner's email, or None. Checks a real
    Firebase ID token (Authorization: Bearer <token>, minted by the SAME
    Google Sign-In already live for Purchases/Materials) against the exact
    same rumee_users/{email}.role == 'owner' check firestore.rules' own
    isOwner() applies -- so this service and the rules agree on who's
    allowed, using one real identity system instead of a shared secret.
    A stolen ID token expires within the hour and can't be reissued by
    anyone who isn't actually signed into the real Google account -- unlike
    a static token, which (being shipped in this file's own public source)
    never expired and couldn't distinguish a real user from anyone who'd
    viewed page source (independent review finding, 2026-07-27).
    """
    header = request.headers.get('Authorization', '')
    if not header.startswith('Bearer '):
        return None
    token = header[7:]
    try:
        decoded = fb_auth.verify_id_token(token)
    except Exception:
        return None
    email = decoded.get('email')
    if not email:
        return None
    try:
        doc = db.collection('rumee_users').document(email).get()
    except Exception:
        return None
    if doc.exists and (doc.to_dict() or {}).get('role') == 'owner':
        return email
    return None


@app.after_request
def add_cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGIN
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    return resp


@app.route('/pm/ensure-doc', methods=['OPTIONS'])
@app.route('/pm/restore-doc', methods=['OPTIONS'])
@app.route('/pm/remove-listings', methods=['OPTIONS'])
@app.route('/pm/delete-doc', methods=['OPTIONS'])
def preflight():
    return ('', 204)


@firestore.transactional
def _ensure_doc_txn(transaction, doc_id, target_sku_id, target_key, design, variation):
    """Runs the whole check-then-create as one Firestore transaction so two
    concurrent calls that resolve to the SAME label but DIFFERENT literal
    doc_ids (e.g. differing only in casing/whitespace) can't both create a
    doc -- whichever transaction commits second sees its collection-wide
    read invalidated by the first's write and is retried by the
    @firestore.transactional wrapper, which then finds the just-created
    doc via the label scan instead of creating a duplicate.
    """
    doc_ref = db.collection('product_master').document(doc_id)
    snap = doc_ref.get(transaction=transaction)
    if snap.exists:
        d = snap.to_dict() or {}
        return {'sku_id': d.get('sku_id', target_sku_id), 'doc_id': doc_id, 'created': False}

    for other in db.collection('product_master').stream(transaction=transaction):
        d = other.to_dict() or {}
        if pm_label_key(d.get('design'), d.get('variation_type')) == target_key:
            return {'sku_id': d.get('sku_id', other.id), 'doc_id': other.id, 'created': False}

    if not _rate_limit_ok():
        raise RuntimeError('rate limit exceeded')

    now = datetime.now(timezone.utc).isoformat()
    payload = {
        'sku_id': target_sku_id,
        'design': design,
        'variation_type': variation,
        'status': 'active',
        'listings': [],
        'created_at': now,
    }
    transaction.create(doc_ref, payload)
    log.info('CREATE %s via ensure-doc', doc_id)
    return {'sku_id': target_sku_id, 'doc_id': doc_id, 'created': True}


@app.route('/pm/ensure-doc', methods=['POST'])
def ensure_doc():
    """Create-if-missing for a genuinely NEW (design, variation) -- does a
    live fuzzy label match first so it never creates a duplicate for a
    variation that already exists under a different literal doc_id. Use
    this whenever the caller does NOT already know the exact doc_id it
    means (Reassign/Rename's "move to a new variation" case)."""
    owner_email = _verify_owner()
    if not owner_email:
        return jsonify({'error': 'unauthorized'}), 401

    body = request.get_json(force=True, silent=True) or {}
    design = (body.get('design') or '').strip()
    variation = (body.get('variation') or '').strip()
    if not design:
        return jsonify({'error': 'design is required'}), 400
    if len(design) > 80 or len(variation) > 80:
        return jsonify({'error': 'design/variation too long'}), 400

    target_sku_id = pm_folder_name(design, variation)
    if not target_sku_id:
        return jsonify({'error': 'resolved empty sku_id'}), 400
    target_key = pm_label_key(design, variation)
    doc_id = re.sub(r'[/. ]', '_', target_sku_id)

    try:
        transaction = db.transaction()
        result = _ensure_doc_txn(transaction, doc_id, target_sku_id, target_key, design, variation)
        return jsonify(result)
    except RuntimeError as e:
        if str(e) == 'rate limit exceeded':
            return jsonify({'error': 'rate limit exceeded, try again later'}), 429
        raise


@app.route('/pm/restore-doc', methods=['POST'])
def restore_doc():
    """Recreates an EXACT known doc_id that the caller believes does NOT
    currently exist (Undo reversing a prior merge/move that deleted it).
    Uses Firestore's atomic create() (fails if the doc already exists) --
    a plain merge=True .set() here would silently overwrite a REAL,
    unrelated live doc if doc_id ever collided with one. Refusing outright
    (409) when the doc already exists is deliberate: silently merging into
    a real live doc during an Undo would be worse than the Undo simply
    failing loudly.
    """
    owner_email = _verify_owner()
    if not owner_email:
        return jsonify({'error': 'unauthorized'}), 401

    body = request.get_json(force=True, silent=True) or {}
    doc_id = (body.get('doc_id') or '').strip()
    fields = body.get('fields')
    if not doc_id or '/' in doc_id:
        return jsonify({'error': 'doc_id is required and must not contain "/"'}), 400
    if not isinstance(fields, dict) or not fields:
        return jsonify({'error': 'fields must be a non-empty object'}), 400
    bad_keys = set(fields.keys()) - ALLOWED_FIELDS
    if bad_keys:
        return jsonify({'error': f'fields not allowed: {sorted(bad_keys)}'}), 400
    if 'listings' in fields and not isinstance(fields['listings'], list):
        return jsonify({'error': 'listings must be an array'}), 400

    if not _rate_limit_ok():
        return jsonify({'error': 'rate limit exceeded, try again later'}), 429

    doc_ref = db.collection('product_master').document(doc_id)
    try:
        doc_ref.create(fields)
    except gcloud_exceptions.AlreadyExists:
        return jsonify({'error': 'doc already exists -- restore aborted to avoid overwriting live data'}), 409
    log.info('RESTORE %s fields=%s by=%s', doc_id, sorted(fields.keys()), owner_email)
    return jsonify({'doc_id': doc_id, 'restored': True})


@app.route('/pm/remove-listings', methods=['POST'])
def remove_listings():
    """Removes specific entries (by product_id/catalog_id key) from an
    EXISTING doc's `listings` array -- firestore.rules' update path blocks
    any shrink at all (append-only). Takes WHICH KEYS to remove, not a full
    replacement array, and computes the new array from a FRESH read taken
    at write time, so anything added concurrently by something else (e.g.
    the daily pipeline) between read and write survives automatically
    instead of being silently clobbered.
    """
    owner_email = _verify_owner()
    if not owner_email:
        return jsonify({'error': 'unauthorized'}), 401

    body = request.get_json(force=True, silent=True) or {}
    doc_id = (body.get('doc_id') or '').strip()
    remove_keys = body.get('remove_keys')
    if not doc_id:
        return jsonify({'error': 'doc_id is required'}), 400
    if not isinstance(remove_keys, list) or not remove_keys or not all(isinstance(k, str) and k for k in remove_keys):
        return jsonify({'error': 'remove_keys must be a non-empty array of non-empty strings'}), 400

    doc_ref = db.collection('product_master').document(doc_id)
    snap = doc_ref.get()
    if not snap.exists:
        return jsonify({'error': 'doc does not exist'}), 404
    current_listings = (snap.to_dict() or {}).get('listings') or []
    remove_set = set(remove_keys)
    new_listings = [l for l in current_listings if _listing_key(l) not in remove_set]
    if len(new_listings) == len(current_listings):
        return jsonify({'error': 'none of remove_keys matched any current listing -- nothing to do'}), 400

    if not _rate_limit_ok():
        return jsonify({'error': 'rate limit exceeded, try again later'}), 429

    doc_ref.set({'listings': new_listings}, merge=True)
    log.info('REMOVE_LISTINGS %s %d -> %d (keys=%s) by=%s', doc_id, len(current_listings), len(new_listings), remove_keys, owner_email)
    return jsonify({'doc_id': doc_id, 'written': True, 'remaining': len(new_listings)})


@app.route('/pm/delete-doc', methods=['POST'])
def delete_doc():
    """Deletes a product_master doc outright (Admin SDK) -- firestore.rules
    blocks this unconditionally, even for a signed-in owner. Used only for
    the internal "this doc's content has already been moved/merged/
    restored elsewhere" cleanup step of Rename/Reassign/Undo -- never
    exposed as a general end-user delete button. Every deleted doc's full
    content is archived to `product_master_deleted` first, so a delete is
    never actually permanent (Golden Rule 9 -- prefer recoverable over
    permanent).
    """
    owner_email = _verify_owner()
    if not owner_email:
        return jsonify({'error': 'unauthorized'}), 401

    body = request.get_json(force=True, silent=True) or {}
    doc_id = (body.get('doc_id') or '').strip()
    if not doc_id:
        return jsonify({'error': 'doc_id is required'}), 400

    if not _rate_limit_ok():
        return jsonify({'error': 'rate limit exceeded, try again later'}), 429

    ref = db.collection('product_master').document(doc_id)
    snap = ref.get()
    if not snap.exists:
        return jsonify({'doc_id': doc_id, 'deleted': False, 'note': 'did not exist'})

    archived = snap.to_dict() or {}
    archived['_deleted_doc_id'] = doc_id
    archived['_deleted_at'] = datetime.now(timezone.utc).isoformat()
    archived['_deleted_by'] = owner_email
    db.collection('product_master_deleted').document(f'{doc_id}_{int(time.time())}').set(archived)
    ref.delete()
    log.info('DELETE %s (archived first) by=%s', doc_id, owner_email)
    return jsonify({'doc_id': doc_id, 'deleted': True})


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000)
