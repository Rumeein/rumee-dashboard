#!/usr/bin/env python3
"""
update_devboard.py — Auto-regenerates the Dev Board section in index.html
from Claude memory files, then commits and pushes to GitHub.

Runs as a PostToolUse hook after Write/Edit on any memory file.
Also callable manually: python update_devboard.py
"""
import sys, json, os, re, glob, subprocess
from datetime import date

MEMORY_DIR   = r"C:\Users\jaisw\.claude\projects\D--Claude-RuMee-Dashbord\memory"
DASHBOARD    = r"D:\Claude RuMee Dashbord\index.html"
DASH_DIR     = r"D:\Claude RuMee Dashbord"
MARKER_START = "<!-- DEV_BOARD_START -->"
MARKER_END   = "<!-- DEV_BOARD_END -->"


def is_memory_file(path):
    norm = path.replace("\\", "/")
    return "memory" in norm and norm.endswith(".md")


def should_run():
    """Return True if we should proceed (memory file was the trigger, or manual run)."""
    if sys.stdin.isatty():
        return True   # manual run — always proceed
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            return True   # empty stdin — proceed
        data = json.loads(raw)
        fp = data.get("tool_input", {}).get("file_path", "")
        return is_memory_file(fp)
    except Exception:
        return True   # parse failure — proceed to be safe


def parse_problem(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()

    def field(key):
        m = re.search(rf"^{key}:\s*(.+)", text, re.MULTILINE | re.IGNORECASE)
        return m.group(1).strip() if m else ""

    # PENDING can be multi-line — grab until next ALL-CAPS field or EOF
    m = re.search(r"^PENDING:\s*(.+?)(?=\n[A-Z ]+:|$)", text, re.MULTILINE | re.DOTALL)
    pending = " ".join(m.group(1).split()) if m else field("PENDING")

    m2 = re.search(r"^OUTCOME:\s*(.+?)(?=\n[A-Z ]+:|$)", text, re.MULTILINE | re.DOTALL)
    outcome = " ".join(m2.group(1).split()) if m2 else ""

    return {
        "problem":      field("PROBLEM"),
        "status":       field("STATUS").lower(),
        "pending":      pending,
        "outcome":      outcome,
        "last_updated": field("LAST UPDATED"),
        "slug":         os.path.basename(path).replace("problem_", "").replace(".md", ""),
    }


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def badge(label, color):
    return (f'<span style="font-size:10px;font-family:var(--mono);background:{color};'
            f'color:#fff;padding:2px 7px;border-radius:10px;white-space:nowrap;margin-top:2px">'
            f'{label}</span>')


def inprogress_card(p):
    b = badge("extension", "var(--warn)")
    title = esc(p["problem"].split("—")[0].strip() if "—" in p["problem"] else p["problem"][:60])
    # Use the part after the first sentence for a cleaner title when too long
    if len(title) > 70:
        title = title[:67] + "…"
    next_step = esc(p["pending"][:120] + ("…" if len(p["pending"]) > 120 else "")) if p["pending"] else ""
    hint = esc(p["slug"].replace("_", " "))
    updated = esc(p["last_updated"])
    next_html = (f'      <div style="font-size:11px;font-family:var(--mono);color:var(--warn);margin-bottom:3px">'
                 f'Next → {next_step}</div>\n') if next_step else ""
    return (
        f'    <div style="border:1px solid var(--accent-border);background:var(--warn-bg);'
        f'border-radius:8px;padding:11px 13px;margin-bottom:8px">\n'
        f'      <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:5px">\n'
        f'        {b}\n'
        f'        <span style="font-size:13px;font-weight:600;color:var(--text)">{title}</span>\n'
        f'      </div>\n'
        f'{next_html}'
        f'      <div style="font-size:11px;font-family:var(--mono);color:var(--muted2)">'
        f'{hint} · last updated {updated}</div>\n'
        f'    </div>'
    )


def resolved_card(p):
    b = badge("extension", "var(--ok)")
    title = esc(p["problem"].split("—")[0].strip() if "—" in p["problem"] else p["problem"][:60])
    if len(title) > 70:
        title = title[:67] + "…"
    outcome = esc(p["outcome"][:100] + ("…" if len(p["outcome"]) > 100 else "")) if p["outcome"] else ""
    updated = esc(p["last_updated"])
    outcome_html = (f'      <div style="font-size:11px;font-family:var(--mono);color:var(--muted2)">'
                    f'{outcome} · {updated}</div>\n') if outcome else (
                    f'      <div style="font-size:11px;font-family:var(--mono);color:var(--muted2)">'
                    f'Resolved {updated}</div>\n')
    return (
        f'    <div style="border:1px solid var(--ok-border);background:var(--ok-bg);'
        f'border-radius:8px;padding:11px 13px;margin-bottom:8px">\n'
        f'      <div style="display:flex;align-items:flex-start;gap:8px;margin-bottom:4px">\n'
        f'        {b}\n'
        f'        <span style="font-size:13px;font-weight:600;color:var(--text)">{title}</span>\n'
        f'      </div>\n'
        f'{outcome_html}'
        f'    </div>'
    )


def generate_devboard():
    files = sorted(glob.glob(os.path.join(MEMORY_DIR, "problem_*.md")))
    problems = [parse_problem(f) for f in files]

    in_progress = [p for p in problems
                   if any(s in p["status"] for s in ["in-progress", "in progress", "blocked"])]
    resolved    = [p for p in problems if "resolved" in p["status"]]
    # Show latest 5 resolved (sorted by last_updated desc)
    resolved_show = sorted(resolved, key=lambda p: p["last_updated"], reverse=True)[:5]

    today = date.today().strftime("%Y-%m-%d")
    parts = [
        MARKER_START,
        '  <!-- ─── Dev Board ─── -->',
        '  <div class="card" style="margin-bottom:14px">',
        '    <div style="display:flex;align-items:center;justify-content:space-between;'
        'margin-bottom:.9rem;flex-wrap:wrap;gap:8px">',
        '      <div class="ctit" style="margin-bottom:0">Dev Board</div>',
        f'      <span style="font-size:10px;font-family:var(--mono);color:var(--muted2)">'
        f'Updated {today} · synced from Claude memory</span>',
        '    </div>',
    ]

    if in_progress:
        parts.append(
            '    <div style="font-size:10px;font-family:var(--mono);color:var(--warn);'
            'text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">● In Progress</div>'
        )
        for p in in_progress:
            parts.append(inprogress_card(p))

    if resolved_show:
        if in_progress:
            parts.append('    <div style="height:8px"></div>')
        parts.append(
            '    <div style="font-size:10px;font-family:var(--mono);color:var(--ok);'
            'text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">✓ Resolved</div>'
        )
        for p in resolved_show:
            parts.append(resolved_card(p))

    if not in_progress and not resolved_show:
        parts.append(
            '    <div style="color:var(--muted);font-size:13px;padding:12px 0;text-align:center">'
            'No open problems — all clear.</div>'
        )

    parts.extend(['  </div>', MARKER_END])
    return "\n".join(parts)


def patch_html(new_section):
    with open(DASHBOARD, encoding="utf-8") as f:
        content = f.read()
    start = content.find(MARKER_START)
    end   = content.find(MARKER_END)
    if start == -1 or end == -1:
        print("[devboard] ERROR: markers not found in index.html — aborting", file=sys.stderr)
        sys.exit(1)
    end += len(MARKER_END)
    patched = content[:start] + new_section + content[end:]
    with open(DASHBOARD, "w", encoding="utf-8") as f:
        f.write(patched)


def git_push():
    def run(args):
        r = subprocess.run(args, cwd=DASH_DIR, capture_output=True, text=True)
        out = (r.stdout + r.stderr).strip()
        return r.returncode, out

    code, out = run(["git", "add", "index.html"])
    if code != 0:
        print(f"[devboard] git add failed: {out}", file=sys.stderr)
        return

    code, out = run(["git", "commit", "-m", f"auto: dev board update {date.today()}"])
    if code != 0:
        if any(s in out for s in ["nothing to commit", "nothing added to commit",
                                   "Changes not staged", "no changes added"]):
            print("[devboard] No changes — nothing to push.")
            return
        print(f"[devboard] git commit failed: {out}", file=sys.stderr)
        return

    code, out = run(["git", "push"])
    if code != 0:
        print(f"[devboard] git push failed: {out}", file=sys.stderr)
    else:
        print(f"[devboard] Pushed. Dev Board updated {date.today()}.")


if __name__ == "__main__":
    if not should_run():
        sys.exit(0)
    html = generate_devboard()
    patch_html(html)
    git_push()
