"""
One-off / re-runnable: extract Flipkart bulk-listing template rules into a
structured, git-tracked reference file under fk_listing_rules/<category>.json.

Reads the seller's own downloaded template (.xls, xlrd/BIFF8 format) and
derives, per column on the category's data-entry sheet:
  - key (machine-friendly slug), display name, data type, multi-value flag
  - mandatory tier, from the ACTUAL cell background color (not FK's Summary
    Sheet prose, which was found to disagree with real colors in this file
    for at least columns E/F -- see NOTES below)
  - dropdown allowed values, from either the 'Index' sheet or a dedicated
    'DropDownValuesForColumnNN' sheet (matched by column position)
  - FK's own example value and description text (rows 2 and 3)
Also captures MatchingAttributes / VariantAttributes (variant-grouping
rule) and the template_version id.

This does NOT touch Firestore, Drive, or any live platform -- pure local
file read + local JSON write. See active.md item #93 for full design
context.

Usage:
    python extract_fk_template_rules.py <path_to_template.xls> <category>

Example:
    python extract_fk_template_rules.py \
        "C:\\Users\\jaisw\\Downloads\\C_earring_....xls" earring
"""

import json
import re
import sys
from pathlib import Path

import xlrd

# RGB -> tier, confirmed against the real "earring" template's header row
# this session (active.md item #93). FK's Summary Sheet prose describes a
# 5th "grey but seller-editable for Passed listings" case for columns E/F
# (Product Data Status / Disapproval Reason) -- but in THIS file those two
# columns are colored BLUE (mandatory), not grey. Trusting the actual cell
# color here since that's what genuinely drives the template's own
# behavior; the prose mismatch is recorded in `notes` below instead of
# silently "corrected" either way.
TIER_BY_RGB = {
    (192, 192, 192): "system",       # Flipkart-filled only, never seller input
    (141, 180, 226): "mandatory",    # must fill or listing fails
    (204, 153, 255): "conditional",  # mandatory unless Fulfilment=Flipkart
    (148, 208, 80): "optional",      # good-to-have
}

NOTES = [
    "FK's own Summary Sheet says columns E/F (Product Data Status, "
    "Disapproval Reason) are 'grey' cells sellers may edit for Passed "
    "listings -- in this actual file they are colored BLUE (mandatory "
    "tier), not grey. Tier below reflects the real cell color, not the "
    "prose. Re-check if a freshly downloaded template ever disagrees.",
    "DropDownValuesForColumn69 contains the same 44 earring Sub Type "
    "values as DropDownValuesForColumn94, even though column 69 in the "
    "data sheet is 'Emerald Clarity', not Sub Type -- likely a copy-paste "
    "artifact in Flipkart's template generation. Not used as this "
    "column's dropdown_source below; flagging in case column 69's real "
    "in-app dropdown turns out to be broken/wrong on Flipkart's side.",
]


def slugify(name):
    s = re.sub(r"[^\w]+", "_", name.strip().lower())
    return re.sub(r"_+", "_", s).strip("_")


def cell_rgb(wb, sheet, row, col):
    xf = wb.xf_list[sheet.cell_xf_index(row, col)]
    return wb.colour_map.get(xf.background.pattern_colour_index)


def sheet_values(wb, name, col=0, start_row=0):
    if name not in wb.sheet_names():
        return []
    sh = wb.sheet_by_name(name)
    out = []
    for r in range(start_row, sh.nrows):
        v = sh.cell_value(r, col)
        if v not in ("", None):
            out.append(v)
    return out


def index_sheet_dropdowns(wb):
    """Map attribute name -> allowed values, from the 'Index' sheet's
    (mostly-duplicated) column blocks. Returns {} if no Index sheet."""
    if "Index" not in wb.sheet_names():
        return {}
    sh = wb.sheet_by_name("Index")
    headers = [sh.cell_value(1, c) for c in range(sh.ncols)]
    out = {}
    for c, header in enumerate(headers):
        if not header or header in out:
            continue
        vals = []
        for r in range(2, sh.nrows):
            v = sh.cell_value(r, c)
            if v not in ("", None):
                vals.append(v)
        if vals:
            out[header] = vals
    return out


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    src_path = Path(sys.argv[1])
    category = sys.argv[2].strip().lower()

    wb = xlrd.open_workbook(str(src_path), formatting_info=True)
    if category not in wb.sheet_names():
        print(f"ERROR: no sheet named '{category}' in this file. "
              f"Sheets found: {wb.sheet_names()}")
        sys.exit(1)

    sh = wb.sheet_by_name(category)
    index_dropdowns = index_sheet_dropdowns(wb)

    template_version = None
    if "template_version" in wb.sheet_names():
        tv_sheet = wb.sheet_by_name("template_version")
        vals = [tv_sheet.cell_value(0, c) for c in range(tv_sheet.ncols)]
        vals = [v for v in vals if v not in ("", None)]
        template_version = vals[-1] if vals else None

    matching_attributes = sheet_values(wb, "MatchingAttributes")
    variant_attributes = sheet_values(wb, "VariantAttributes")

    columns = []
    for c in range(sh.ncols):
        name = sh.cell_value(0, c)
        if not name:
            continue
        data_type_raw = sh.cell_value(1, c) or ""
        example = sh.cell_value(2, c)
        description = sh.cell_value(3, c)
        rgb = cell_rgb(wb, sh, 0, c)
        tier = TIER_BY_RGB.get(rgb, "unknown")

        dropdown_source = None
        dropdown_values = None
        dedicated_sheet = f"DropDownValuesForColumn{c}"
        if dedicated_sheet in wb.sheet_names():
            vals = sheet_values(wb, dedicated_sheet)
            if vals:
                dropdown_source = dedicated_sheet
                dropdown_values = vals
        if dropdown_values is None and name in index_dropdowns:
            dropdown_source = "Index"
            dropdown_values = index_dropdowns[name]

        columns.append({
            "index": c,
            "key": slugify(name),
            "name": name,
            "data_type": data_type_raw.split("\n")[0].strip(),
            "multi_value": "MULTI" in data_type_raw.upper(),
            "tier": tier,
            "cell_rgb": list(rgb) if rgb else None,
            "example": example if example != "" else None,
            "description": description if description != "" else None,
            "dropdown_source": dropdown_source,
            "dropdown_values": dropdown_values,
        })

    out = {
        "category": category,
        "template_version": template_version,
        "source_file": src_path.name,
        "data_sheet": category,
        "data_start_row": 5,  # row index 4 (0-based) = Excel row 5, first blank data row
        "columns": columns,
        "matching_attributes": matching_attributes,
        "variant_attributes": variant_attributes,
        "notes": NOTES,
    }

    out_dir = Path(__file__).parent / "fk_listing_rules"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"{category}.json"
    out_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    tiers = {}
    for col in columns:
        tiers[col["tier"]] = tiers.get(col["tier"], 0) + 1
    print(f"Wrote {out_path} -- {len(columns)} columns, tiers: {tiers}")
    print(f"template_version: {template_version}")
    print(f"matching_attributes: {len(matching_attributes)}, variant_attributes: {variant_attributes}")


if __name__ == "__main__":
    main()
