"""
Generate product_master.csv from rumee_db_daily.csv.
Auto-classifies design / variation / platform for each SKU.
Flags ambiguous ones as needs_review.
"""
import csv, re, json

# ── helpers ──────────────────────────────────────────────────────────────────

DESIGN_PREFIXES = [
    'PMC','BBO','MJC','ATH','OEC',
    'OE','DJ','DN','NJ','YJ','GB','RC','DC','DM','SC','ME','BB','OC','OG',
    'JC','NJ',
]

COLORS = ['BLUE','PINK','PURPLE','GREEN','RED','ORANGE','BLACK','SILVER','GOLD','KASHMIRI']
SIZE_WORDS = ['SMALL','MINI','LARGE','JUMBO']

def get_platform(sku_id, tables):
    sid = sku_id.lower()
    if sid.startswith('fk-') or sid.endswith('-fk'):
        return 'fk'
    if sid.startswith('me-') or sid.endswith('-me'):
        return 'me'
    if 'fk_daily' in tables and 'me_daily' not in tables:
        return 'fk'
    if 'me_daily' in tables and 'fk_daily' not in tables:
        return 'me'
    if 'fk_daily' in tables and 'me_daily' in tables:
        return 'both'
    return 'unknown'

def get_variation(name):
    n = name.upper()
    is_og = bool(re.search(r'\bOG\b', n))
    is_bahu = 'BAHUBALI' in n or 'BAHU' in n
    colors = [c.title() for c in COLORS if re.search(r'\b' + c + r'\b', n)]
    if is_og:
        return 'OG'
    if is_bahu:
        return 'Bahubali'
    if colors:
        return colors[0]
    return 'Base'

def extract_design_from_text(text):
    """Extract design code like DJ-11, DN-5, PMC-3 from arbitrary text."""
    t = text.upper()
    # Multi-letter codes with number
    for pfx in DESIGN_PREFIXES:
        m = re.search(r'\b' + pfx + r'[-\s]?(\d{1,2})\b', t)
        if m:
            return f"{pfx}-{m.group(1)}"
    # Single-letter + number codes: E-17, J-7, S-5678
    m = re.search(r'\b([A-Z])-(\d{1,4})\b', t)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None

def extract_design_from_sku(sku_id):
    """Fallback: extract design from sku_id string."""
    sid = sku_id.lower()
    # Strip platform prefixes
    for pfx in ('fk-', 'me-'):
        if sid.startswith(pfx):
            sid = sid[len(pfx):]
    # Strip 'og' prefix
    if sid.startswith('og'):
        sid = sid[2:]
    # Strip platform suffixes
    for sfx in ('-me', '-fk'):
        if sid.endswith(sfx):
            sid = sid[:-len(sfx)]
    # Strip trailing variation letters: b, p (e.g. dj11b, dj12p)
    sid = re.sub(r'(\d+)[bp]\d*$', r'\1', sid)
    # Must start at a word boundary (beginning or after non-alpha)
    # Require 2-4 alpha chars at start of a word segment, then optional dash, then digits
    m = re.search(r'(?:^|[^a-z])([a-z]{2,4})-?(\d{1,2})(?:$|[^a-z0-9])', sid)
    if m:
        return f"{m.group(1).upper()}-{m.group(2)}"
    return None

def categorize(sku_id, sku_name, tables):
    platform = get_platform(sku_id, tables)
    variation = get_variation(sku_name)

    # Try name first, then sku_id
    design = extract_design_from_text(sku_name)
    if not design:
        design = extract_design_from_sku(sku_id)

    # Special named products — check BEFORE generic fallback to avoid misparse
    name_up = sku_name.upper()
    if not design:
        # NJ without a number: "NJ Mini", "NJ Small" → small variation of NJ-2
        if re.search(r'\bNJ\b', name_up) and re.search(r'\b(SMALL|MINI)\b', name_up):
            design = 'NJ-2'
        # "DJ Bahu" with no design number → DJ-1 Bahubali (big Bahubali; Jaiswal confirmed)
        elif re.search(r'\bDJ\b', name_up) and re.search(r'\bBAHU', name_up) and not re.search(r'\bDJ[-\s]?\d', name_up):
            design = 'DJ-1'
        # J-series range combos: "J11 - J13" → combo of J11, J12, J13 (requires spaces around dash)
        elif re.search(r'\bJ(\d{1,2})\s+[-–]+\s+J?(\d{1,2})\b', name_up):
            m = re.search(r'\bJ(\d{1,2})\s+[-–]+\s+J?(\d{1,2})\b', name_up)
            design = f"J{m.group(1)}-J{m.group(2)}"
            variation = 'Base'
        # J-series single: "J1-1", "J2" → J-1, J-2 (treat like OE: Base or Bahubali)
        elif re.match(r'^J(\d{1,2})(-\d+)?$', name_up.strip()):
            m = re.match(r'^J(\d{1,2})', name_up.strip())
            design = f"J-{m.group(1)}"
        # RC combos: RC-B, RC-R — each is its own standalone combo product
        elif re.match(r'^RC-[A-Z]$', name_up.strip()):
            design = name_up.strip()
            variation = 'Base'
        elif 'KAMARBAND' in name_up:  design = 'KAMARBAND'
        elif 'BANGLE' in name_up:   design = 'BANGLE'
        elif 'BRACELET' in name_up: design = 'BRACELET'
        elif 'COMBO' in name_up:
            # Each combo name = unique design. Strip parenthetical listing suffixes like "(1)".
            clean = re.sub(r'\s*\(\d+\)\s*$', '', name_up).strip()
            clean = re.sub(r'[\s-]+', '-', clean)
            design = re.sub(r'[^A-Z0-9-]', '', clean).strip('-')
        elif 'LOTUS JHUMKA' in name_up:
            m = re.search(r'LOTUS JHUMKA[-\s]?(\d)', name_up)
            design = f"LOTUS-JHUMKA-{m.group(1)}" if m else 'LOTUS-JHUMKA'
        elif 'LAKSHMI' in name_up:  design = 'LAKSHMI-JHUMKA'
        elif 'JUMBO JHUMKA' in name_up: design = 'JUMBO-JHUMKA'
        elif 'TURKMAAN' in name_up: design = 'TURKMAAN-JHUMKA'
        elif 'BLACK JHUMKA' in name_up: design = 'BLACK-JHUMKA'
        elif 'JHUMKA' in name_up:   design = 'JHUMKA'
        elif 'CHOKER' in name_up or 'COIN PEARL' in name_up: design = 'COIN-CHOKER'
        elif 'NECKLACE' in name_up: design = 'NECKLACE'
        elif 'HAATHI' in name_up or 'ELEPHANT' in name_up: design = 'ELEPHANT'
        elif 'BUTTERFLY' in name_up or 'BOW' in name_up:   design = 'BUTTERFLY-BOW'
        elif 'COIN' in name_up:     design = 'COIN-CHOKER'
        elif 'SILVER KASHMIRI' in name_up:
            m = re.search(r'KASHMIRI[-\s]?(\d)', name_up)
            design = f"SILVER-KASHMIRI-{m.group(1)}" if m else 'SILVER-KASHMIRI'

    # J-range combos (J11-J13 etc.) and RC combos — force Base
    if design and re.match(r'^J\d+-J\d+$', design):
        variation = 'Base'
    if design and re.match(r'^RC-[A-Z]$', design):
        variation = 'Base'

    # NJ series: earring is a Bahubali product by nature — bought from market with chain pre-attached.
    # "BAHUBALI" or "OG" in the listing name = product descriptor, NOT a style variation.
    # Only real variation is size: Small/Mini vs Base (full size).
    if design and design.startswith('NJ-'):
        if re.search(r'\b(SMALL|MINI)\b', name_up):
            variation = 'Small'
        else:
            variation = 'Base'

    # Status
    if not design or design == 'UNKNOWN':
        status = 'needs_review'
        design = design or 'UNKNOWN'
    elif variation == 'Base':
        # Base might be legit (single-variation product) or ambiguous
        # Mark as needs_review only if same design has OG/Bahubali siblings
        status = 'auto'
    else:
        status = 'auto'

    # Combos are always single Base — variation words in the name are part of the product name, not a style variant
    if design and 'COMBO' in design.upper():
        variation = 'Base'

    return {
        'design':    design or 'UNKNOWN',
        'variation': variation,
        'sku_id':    sku_id,
        'sku_name':  sku_name,
        'platform':  platform,
        'status':    status,
        'notes':     '',
    }

# ── load ─────────────────────────────────────────────────────────────────────
skus = {}
with open(r'D:\Claude RuMee Dashbord\rumee_db_daily.csv', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        sid = row.get('sku_id','').strip()
        if not sid or sid == 'sku_id':
            continue
        if sid not in skus:
            skus[sid] = {'sku_name': row.get('sku_name','').strip(), 'tables': set()}
        skus[sid]['tables'].add(row.get('__table__','').strip())

# ── classify ──────────────────────────────────────────────────────────────────
rows = []
for sid, info in sorted(skus.items()):
    rows.append(categorize(sid, info['sku_name'], info['tables']))

# ── second pass: flag Base listings where OG/Bahubali sibling exists ──────────
design_variations = {}
for r in rows:
    d = r['design']
    if d not in design_variations:
        design_variations[d] = set()
    design_variations[d].add(r['variation'])

for r in rows:
    d = r['design']
    vars_for_design = design_variations.get(d, set())
    if d == 'UNKNOWN':
        # Already needs_review; don't add misleading "sibling" notes from other unknowns
        r['status'] = 'needs_review'
        r['notes'] = 'Design could not be auto-detected'
    elif r['variation'] == 'Base' and len(vars_for_design) > 1:
        # Only flag if sibling is OG or Bahubali — Base is ambiguous alongside those.
        # Size siblings (Small, Mini) are known/confirmed, not ambiguous.
        ambiguous_siblings = {v for v in vars_for_design if v not in ('Base', 'Small', 'Mini')}
        if ambiguous_siblings:
            r['status'] = 'needs_review'
            r['notes'] = f"Other variations exist: {', '.join(sorted(ambiguous_siblings))}"

# ── write ─────────────────────────────────────────────────────────────────────
out_path = r'D:\Claude RuMee Dashbord\product_master.csv'
fieldnames = ['design','variation','sku_id','sku_name','platform','status','notes']
with open(out_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    w.writerows(rows)

# ── summary ───────────────────────────────────────────────────────────────────
total = len(rows)
auto = sum(1 for r in rows if r['status'] == 'auto')
needs = sum(1 for r in rows if r['status'] == 'needs_review')
designs = len(set(r['design'] for r in rows))

print(f"Written: {out_path}")
print(f"Total SKUs   : {total}")
print(f"Auto-classified : {auto}")
print(f"Needs review    : {needs}")
print(f"Unique designs  : {designs}")
print()
print("--- NEEDS REVIEW ---")
for r in rows:
    if r['status'] == 'needs_review':
        print(f"  {r['sku_id']:40s} | design={r['design']:15s} | var={r['variation']:12s} | {r['notes']}")
