#!/usr/bin/env python3
"""
Input: 1 TXT file where each line contains an EC number and an enzyme name.
Output: 1 TSV file where entries are grouped (O-MT first, then N-MT, C-MT, S-MT, ...).

Example input lines the parser accepts:
  2.1.1.1 nicotinamide N-methyltransferase
  2.1.1.1, nicotinamide N-methyltransferase
  2.1.1.63methylated-DNA--[protein]-cysteine S-methyltransferase   # no space after EC
  2.1.1.123 Transferred entry: ...
  2.1.1.456 Deleted entry

Run:
  python group_mt_by_atom_onefile.py enzyme_lines.txt
  python group_mt_by_atom_onefile.py enzyme_lines.txt --out grouped.tsv
"""

import argparse
import re
from pathlib import Path

# --- Parse: EC + optional comma + optional/no whitespace + rest-of-line as "name" ---
EC_LINE = re.compile(r"^\s*(\d+\.\d+\.\d+\.\d+)\s*,?\s*(.*)\s*$")

# --- Classification patterns (name-based heuristics) ---
RE_2P_O = re.compile(r"2['′]-o", re.IGNORECASE)               # 2'-O / 2′-O
RE_O_MT = re.compile(r"\bo-methyltransferase\b", re.IGNORECASE)

RE_N_MT = re.compile(r"\bn-methyltransferase\b", re.IGNORECASE)
RE_N_PAREN = re.compile(r"\bn\(\d+\)|n\(alpha\)", re.IGNORECASE)  # N(2), N(7), N(alpha)

RE_C_MT = re.compile(r"\bc-methyltransferase\b", re.IGNORECASE)
RE_C_PAREN = re.compile(r"\bc\(\d+\)", re.IGNORECASE)          # C(5)
RE_CYTOSINE_5 = re.compile(r"cytosine-5", re.IGNORECASE)

RE_S_MT = re.compile(r"\bs-methyltransferase\b", re.IGNORECASE)
RE_S_HINT = re.compile(r"\bthiol\b|\bthioether\b|\bcysteine\b|\bmercaptan\b", re.IGNORECASE)

# Avoid confusing "Co-methyltransferase" with C-methyltransferase
RE_CO_MT = re.compile(r"\bco-methyltransferase\b", re.IGNORECASE)

RE_MT_WORD = re.compile(r"\bmethyltransferase\b", re.IGNORECASE)


def classify(name: str, status: str) -> str:
    if status == "TRANSFERRED":
        return "TRANSFERRED"
    if status == "DELETED":
        return "DELETED"

    if RE_CO_MT.search(name):
        return "OTHER"

    if RE_2P_O.search(name) or RE_O_MT.search(name):
        return "O_MT"

    if RE_N_MT.search(name) or RE_N_PAREN.search(name):
        return "N_MT"

    if RE_C_MT.search(name) or RE_C_PAREN.search(name) or RE_CYTOSINE_5.search(name):
        return "C_MT"

    low = name.lower()
    if RE_S_MT.search(name) or (RE_MT_WORD.search(name) and RE_S_HINT.search(name) and "s-adenosyl" not in low):
        return "S_MT"

    if RE_MT_WORD.search(name):
        return "UNCLEAR"

    return "OTHER"


def ec_sort_key(ec: str):
    # numeric sort by EC parts
    try:
        return [int(x) for x in ec.split(".")]
    except Exception:
        return [999, 999, 999, 999]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_txt", help="TXT file: each line = 'EC <name>'")
    ap.add_argument("--out", default=None, help="Output TSV path (default: <input_stem>_grouped.tsv)")
    args = ap.parse_args()

    in_path = Path(args.input_txt)
    out_path = Path(args.out) if args.out else in_path.with_name(f"{in_path.stem}_grouped.tsv")

    rows = []
    skipped = 0

    for raw in in_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        m = EC_LINE.match(line)
        if not m:
            skipped += 1
            continue

        ec = m.group(1).strip()
        name = (m.group(2) or "").strip()

        # clean common junk
        name = re.sub(r"\s*Read more\s*$", "", name, flags=re.IGNORECASE).strip()
        name = name.rstrip(" .;,")

        low = name.lower()
        status = "OK"
        if low.startswith("transferred entry"):
            status = "TRANSFERRED"
        elif low.startswith("deleted entry"):
            status = "DELETED"

        group = classify(name, status)
        rows.append({"group": group, "ec": ec, "name": name, "status": status})

    # group order in output TSV
    order = ["O_MT", "N_MT", "C_MT", "S_MT", "UNCLEAR", "OTHER", "TRANSFERRED", "DELETED"]
    order_index = {g: i for i, g in enumerate(order)}

    rows.sort(key=lambda r: (order_index.get(r["group"], 999), ec_sort_key(r["ec"]), r["name"].lower()))

    # write ONE TSV with groups stacked in blocks (and a header per block)
    out_lines = []
    for g in order:
        block = [r for r in rows if r["group"] == g]
        out_lines.append(f"# {g} ({len(block)})")
        out_lines.append("ec\tname\tstatus")
        for r in block:
            # ensure no tabs/newlines in name
            clean_name = r["name"].replace("\t", " ").replace("\n", " ").strip()
            out_lines.append(f"{r['ec']}\t{clean_name}\t{r['status']}")
        out_lines.append("")  # blank line between blocks

    out_path.write_text("\n".join(out_lines).rstrip() + "\n", encoding="utf-8")

    print(f"Parsed lines: {len(rows)}")
    print(f"Skipped lines (couldn't parse EC + name): {skipped}")
    print(f"Wrote: {out_path.resolve()}")


if __name__ == "__main__":
    main()
