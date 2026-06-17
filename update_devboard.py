#!/usr/bin/env python3
"""
update_devboard.py — Regenerates the Dev Tasks page in index.html from Claude
memory files + devboard_config.json.
3-column grid: Dashboard | Daily Sync + Backfill | Bulk Download
Triggered via PostToolUse hook. Also callable manually.
"""
import sys, json, os, re, glob, subprocess
from datetime import date

DASH_MEMORY = r"C:\Users\jaisw\.claude\projects\D--Claude-RuMee-Dashbord\memory"
EXT_MEMORY  = r"C:\Users\jaisw\.claude\projects\D--rumee-auto-sync\memory"
DASHBOARD   = r"D:\Claude RuMee Dashbord\index.html"
DASH_DIR    = r"D:\Claude RuMee Dashbord"
CONFIG_FILE = r"D:\Claude RuMee Dashbord\devboard_config.json"
MARKER_S    = "<!-- DEV_BOARD_START -->"
MARKER_E    = "<!-- DEV_BOARD_END -->"

# ─── Inline CSS + JS (generated once, lives inside markers) ──────────────────

HEAD = """\
<style>
/* Dev Tasks board */
.dt-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;align-items:start}
@media(max-width:900px){.dt-grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:600px){.dt-grid{grid-template-columns:1fr}}
.dt-col-stack{display:flex;flex-direction:column;gap:16px}
.dt-card{background:var(--surf);border:1px solid var(--border);border-radius:var(--r);padding:1rem 1.1rem}
.dt-hdr{display:flex;align-items:center;justify-content:space-between;margin-bottom:.8rem;gap:8px}
.dt-title{font-size:10px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:600}
.dt-toggle{min-width:44px;min-height:44px;display:flex;align-items:center;justify-content:center;
           background:var(--surf2);border:1px solid var(--border);border-radius:8px;
           font-size:18px;color:var(--muted);cursor:pointer;flex-shrink:0;transition:background .15s,color .15s}
.dt-toggle:hover{background:var(--border);color:var(--text)}
.dt-body{}
.dt-card.dt-col>.dt-body{display:none}
.dt-card.dt-col .dt-toggle{color:var(--muted2)}
.dt-row{padding:9px 0;border-bottom:1px solid var(--border)}
.dt-row:last-child{border-bottom:none;padding-bottom:0}
.dt-row-top{display:flex;align-items:flex-start;justify-content:space-between;gap:8px;margin-bottom:3px}
.dt-name{font-size:13px;font-weight:500;color:var(--text);line-height:1.3;flex:1}
.dt-name.done{color:var(--muted);text-decoration:line-through;text-decoration-color:var(--border2)}
.dt-desc{font-size:11px;color:var(--muted);line-height:1.4}
.dt-badge{font-size:9px;font-family:var(--mono);padding:3px 7px;border-radius:8px;
          white-space:nowrap;flex-shrink:0;margin-top:1px;font-weight:600;letter-spacing:.04em}
.dt-badge.open{background:var(--warn-bg);color:var(--warn)}
.dt-badge.done{background:var(--ok-bg);color:var(--ok)}
.dt-badge.halt{background:var(--surf2);color:var(--muted2);border:1px solid var(--border)}
.dt-halted{font-size:12px;font-family:var(--mono);color:var(--muted2);
           background:var(--surf2);border-radius:8px;padding:10px 12px;line-height:1.6}
.dt-sub{font-size:10px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.07em;
        color:var(--muted2);margin:14px 0 8px;padding-top:12px;border-top:1px solid var(--border)}
.dt-sub:first-child{margin-top:0;padding-top:0;border-top:none}
</style>
<script>
(function(){
  var S=JSON.parse(localStorage.getItem('dtState')||'{}');
  function restore(){
    document.querySelectorAll('.dt-card').forEach(function(el){
      if(S[el.id]===true) el.classList.add('dt-col');
      var btn=el.querySelector(':scope>.dt-hdr>.dt-toggle');
      if(btn) btn.textContent=el.classList.contains('dt-col')?'▸':'▾';
    });
  }
  window._dtToggle=function(id){
    var el=document.getElementById(id);
    if(!el)return;
    el.classList.toggle('dt-col');
    var collapsed=el.classList.contains('dt-col');
    S[id]=collapsed;
    localStorage.setItem('dtState',JSON.stringify(S));
    var btn=el.querySelector(':scope>.dt-hdr>.dt-toggle');
    if(btn) btn.textContent=collapsed?'▸':'▾';
  };
  if(document.readyState==='loading'){
    document.addEventListener('DOMContentLoaded',restore);
  } else { restore(); }
})();
</script>"""

# ─── Helpers ──────────────────────────────────────────────────────────────────

def should_run():
    if sys.stdin.isatty():
        return True
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return True
        data = json.loads(raw)
        fp   = data.get("tool_input", {}).get("file_path", "")
        norm = fp.replace("\\", "/")
        return ("memory" in norm and norm.endswith(".md")) or "devboard_config" in norm
    except Exception:
        return True


def parse_file(path):
    try:
        text = open(path, encoding="utf-8").read()
    except Exception:
        return None
    def field(key):
        m = re.search(rf"^{key}:\s*(.+)", text, re.MULTILINE | re.IGNORECASE)
        return m.group(1).strip() if m else ""
    status = field("STATUS").lower()
    if not status:
        return None
    m = re.search(r"^OUTCOME:\s*(.+?)(?=\n[A-Z ]+:|---|\Z)", text, re.MULTILINE | re.DOTALL)
    outcome = " ".join(m.group(1).split()) if m else ""
    return {"problem": field("PROBLEM"), "status": status, "outcome": outcome}


def short_desc(problem, outcome, status):
    if "resolved" in status and outcome:
        return outcome[:95] + ("…" if len(outcome) > 95 else "")
    if not problem:
        return ""
    for sep in [". ", " when ", " — ", " - ", "; "]:
        idx = problem.find(sep)
        if 0 < idx <= 95:
            return problem[:idx].strip()
    return (problem[:85].rsplit(" ", 1)[0] + "…") if len(problem) > 85 else problem


def load_section(section, titles):
    src   = DASH_MEMORY if section["source"] == "dashboard" else EXT_MEMORY
    spec  = section["files"]
    files = sorted(glob.glob(os.path.join(src, spec))) if isinstance(spec, str) else \
            [os.path.join(src, s + ".md") for s in spec]
    rows  = []
    for f in files:
        if not os.path.exists(f):
            continue
        p = parse_file(f)
        if not p or not p["problem"]:
            continue
        slug  = os.path.basename(f).replace(".md", "")
        title = titles.get(slug, slug.replace("_", " "))
        desc  = short_desc(p["problem"], p["outcome"], p["status"])
        rows.append({"title": title, "desc": desc, "status": p["status"]})
    return rows


def esc(s):
    return (s.replace("&","&amp;").replace("<","&lt;")
             .replace(">","&gt;").replace('"',"&quot;"))


def badge(status):
    if "resolved" in status:
        return '<span class="dt-badge done">DONE</span>'
    if "blocked" in status:
        return '<span class="dt-badge halt">BLOCKED</span>'
    return '<span class="dt-badge open">IN PROGRESS</span>'


def row_html(r, last=False):
    resolved  = "resolved" in r["status"]
    name_cls  = "dt-name done" if resolved else "dt-name"
    return (
        f'      <div class="dt-row{"" if not last else " dt-row-last"}">\n'
        f'        <div class="dt-row-top">\n'
        f'          <span class="{name_cls}">{esc(r["title"])}</span>\n'
        f'          {badge(r["status"])}\n'
        f'        </div>\n'
        f'        <div class="dt-desc">{esc(r["desc"])}</div>\n'
        f'      </div>'
    )


def card_html(card_id, title, body, badge_extra=""):
    hdr_badge = (f'<span class="dt-badge halt" style="font-size:9px">{esc(badge_extra)}</span> '
                 if badge_extra else "")
    return (
        f'    <div class="dt-card" id="{card_id}">\n'
        f'      <div class="dt-hdr">\n'
        f'        <div style="display:flex;align-items:center;gap:8px">\n'
        f'          <span class="dt-title">{esc(title)}</span>\n'
        f'          {hdr_badge}\n'
        f'        </div>\n'
        f'        <button class="dt-toggle" onclick="_dtToggle(\'{card_id}\')">▾</button>\n'
        f'      </div>\n'
        f'      <div class="dt-body">\n'
        f'{body}\n'
        f'      </div>\n'
        f'    </div>'
    )


# ─── Generator ────────────────────────────────────────────────────────────────

def generate_devboard():
    cfg    = json.load(open(CONFIG_FILE, encoding="utf-8"))
    titles = cfg.get("titles", {})
    today  = date.today().strftime("%Y-%m-%d")

    sections_by_id = {s["id"]: s for s in cfg["sections"]}

    # ── Column 1: Dashboard ──────────────────────────────────────────────
    dash_sec  = sections_by_id["dashboard"]
    dash_rows = load_section(dash_sec, titles)
    open_r    = [r for r in dash_rows if "resolved" not in r["status"]]
    done_r    = [r for r in dash_rows if "resolved" in r["status"]]
    all_r     = open_r + done_r
    body1     = "\n".join(row_html(r, last=(i==len(all_r)-1)) for i,r in enumerate(all_r))
    col1      = card_html("dt-dashboard", "Dashboard", body1)

    # ── Column 2: Daily Sync + Backfill (stacked) ────────────────────────
    col2_cards = []

    daily_sec  = sections_by_id["ext_daily"]
    daily_rows = load_section(daily_sec, titles)
    open_r     = [r for r in daily_rows if "resolved" not in r["status"]]
    done_r     = [r for r in daily_rows if "resolved" in r["status"]]
    all_r      = open_r + done_r
    body_d     = "\n".join(row_html(r, last=(i==len(all_r)-1)) for i,r in enumerate(all_r))
    col2_cards.append(card_html("dt-daily", "Extension · Daily Sync", body_d))

    bf_sec     = sections_by_id["ext_backfill"]
    bf_rows    = load_section(bf_sec, titles)
    open_r     = [r for r in bf_rows if "resolved" not in r["status"]]
    done_r     = [r for r in bf_rows if "resolved" in r["status"]]
    all_r      = open_r + done_r
    body_bf    = "\n".join(row_html(r, last=(i==len(all_r)-1)) for i,r in enumerate(all_r))
    col2_cards.append(card_html("dt-backfill", "Extension · Backfill", body_bf))

    col2 = (
        '    <div class="dt-col-stack">\n' +
        "\n".join(col2_cards) +
        '\n    </div>'
    )

    # ── Column 3: Bulk (halted — items still shown from memory) ─────────
    bulk_sec   = sections_by_id["ext_bulk"]
    bulk_rows  = load_section(bulk_sec, titles)
    since      = bulk_sec.get("halted_since", "")
    reason     = bulk_sec.get("halted_reason", "paused")
    open_r     = [r for r in bulk_rows if "resolved" not in r["status"]]
    done_r     = [r for r in bulk_rows if "resolved" in r["status"]]
    all_r      = open_r + done_r
    halt_note  = (
        f'      <div class="dt-halted" style="margin-bottom:10px">\n'
        f'        {esc(reason)} · since {esc(since)}\n'
        f'      </div>'
    )
    rows_html  = "\n".join(row_html(r, last=(i==len(all_r)-1)) for i,r in enumerate(all_r))
    bulk_body  = halt_note + "\n" + rows_html
    col3 = card_html("dt-bulk", "Extension · Bulk", bulk_body, badge_extra="HALTED")

    # ── Assemble page ────────────────────────────────────────────────────
    grid = (
        f'  <div style="display:flex;align-items:baseline;justify-content:space-between;'
        f'margin-bottom:1rem;flex-wrap:wrap;gap:8px">\n'
        f'    <h2 style="font-family:var(--disp);font-size:1.4rem;font-weight:500;color:var(--text);margin:0">'
        f'Dev Tasks</h2>\n'
        f'    <span style="font-size:10px;font-family:var(--mono);color:var(--muted2)">Updated {today} · auto-synced from Claude memory</span>\n'
        f'  </div>\n'
        f'  <div class="dt-grid">\n'
        f'{col1}\n'
        f'{col2}\n'
        f'{col3}\n'
        f'  </div>'
    )

    return "\n".join([MARKER_S, HEAD, grid, MARKER_E])


# ─── Patch + push ─────────────────────────────────────────────────────────────

def patch_html(html):
    content = open(DASHBOARD, encoding="utf-8").read()
    s = content.find(MARKER_S)
    e = content.find(MARKER_E) + len(MARKER_E)
    if s == -1 or e == -1:
        print("[devboard] ERROR: markers not found", file=sys.stderr); sys.exit(1)
    open(DASHBOARD, "w", encoding="utf-8").write(content[:s] + html + content[e:])


def git_push():
    def run(args):
        r = subprocess.run(args, cwd=DASH_DIR, capture_output=True, text=True)
        return r.returncode, (r.stdout + r.stderr).strip()
    run(["git", "add", "index.html", "devboard_config.json"])
    code, out = run(["git", "commit", "-m", f"auto: dev board {date.today()}"])
    if code != 0:
        if any(s in out for s in ["nothing to commit","nothing added",
                                   "Changes not staged","no changes added"]):
            print("[devboard] No changes."); return
        print(f"[devboard] commit failed: {out}", file=sys.stderr); return
    code, out = run(["git", "push"])
    if code != 0:
        print(f"[devboard] push failed: {out}", file=sys.stderr)
    else:
        print(f"[devboard] Pushed {date.today()}.")


if __name__ == "__main__":
    if not should_run(): sys.exit(0)
    patch_html(generate_devboard())
    git_push()
