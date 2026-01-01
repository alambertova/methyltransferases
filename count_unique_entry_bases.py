#!/usr/bin/env python3
"""
Count unique "base entry names" in one or many TSV files.

Example:
  POLG_WSLV -> base "POLG"  (everything before the first underscore)

Outputs:
  - per-file: total rows, unique full entry names, unique base names
  - overall across all files

Run:
  python count_unique_entry_bases.py MT2_by_group/*.tsv
  python count_unique_entry_bases.py O_MT.tsv N_MT.tsv C_MT.tsv

If your column is not called "Entry name", use --col.
"""

import argparse
from pathlib import Path
from collections import Counter

import pandas as pd


def base_name(entry: str) -> str:
    """Return the base part before the first underscore."""
    entry = str(entry).strip()
    if not entry:
        return ""
    return entry.split("_", 1)[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+", help="TSV files to analyze")
    ap.add_argument(
        "--col",
        default="Entry Name",
        help='Column containing UniProt entry names (default: "Entry name")',
    )
    ap.add_argument(
        "--top",
        type=int,
        default=15,
        help="Show top N most frequent base names per file and overall (default: 15). Use 0 to skip.",
    )
    args = ap.parse_args()

    overall_full = set()
    overall_base = set()
    overall_base_counts = Counter()

    for f in args.files:
        path = Path(f)
        df = pd.read_csv(path, sep="\t", dtype=str)

        if args.col not in df.columns:
            raise SystemExit(f"ERROR: Column '{args.col}' not found in {path.name}. Columns: {list(df.columns)}")

        entries = df[args.col].fillna("").astype(str).str.strip()
        entries = entries[entries != ""]  # drop empty

        full_set = set(entries.tolist())
        bases = [base_name(x) for x in entries.tolist() if base_name(x)]
        base_set = set(bases)
        base_counts = Counter(bases)

        overall_full |= full_set
        overall_base |= base_set
        overall_base_counts.update(base_counts)

        print(f"\n== {path.name} ==")
        print(f"Rows (non-empty '{args.col}'): {len(entries)}")
        print(f"Unique full entry names:       {len(full_set)}")
        print(f"Unique base names (before _):  {len(base_set)}")

        if args.top and base_counts:
            print(f"Top {args.top} base names:")
            for name, cnt in base_counts.most_common(args.top):
                print(f"  {name}\t{cnt}")

    print("\n== OVERALL (across all files) ==")
    print(f"Unique full entry names:      {len(overall_full)}")
    print(f"Unique base names (before _): {len(overall_base)}")

    if args.top and overall_base_counts:
        print(f"Top {args.top} base names overall:")
        for name, cnt in overall_base_counts.most_common(args.top):
            print(f"  {name}\t{cnt}")


if __name__ == "__main__":
    main()
