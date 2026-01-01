#!/usr/bin/env python3
"""
Split a UniProt methyltransferase table into separate files based on EC number.

- Reads TSV/CSV.
- Extracts EC numbers from a chosen EC column (preferred) OR from "Catalytic activity" text.
- Writes one output file per EC group (including "NO_EC").
- Also writes a summary TSV with counts per EC.

USAGE:
  python split_by_ec.py uniprot_export.tsv
  python split_by_ec.py uniprot_export.tsv --ec-col "EC number"
  python split_by_ec.py uniprot_export.tsv --cat-col "Catalytic activity"  # if EC is embedded in text
  python split_by_ec.py uniprot_export.csv --sep ","
"""

import argparse
import re
from pathlib import Path

import pandas as pd


EC_REGEX = re.compile(r"\b\d+\.\d+\.\d+\.(?:\d+|-)\b")


def guess_sep(path: Path) -> str:
    if path.suffix.lower() in {".tsv", ".tab"}:
        return "\t"
    if path.suffix.lower() == ".csv":
        return ","
    return "\t"


def extract_ec_list(s: str) -> list[str]:
    """Return a sorted unique list of EC numbers found in a string."""
    if not isinstance(s, str):
        return []
    ecs = EC_REGEX.findall(s)
    return sorted(set(ecs))


def sanitize_filename(s: str) -> str:
    """Make a safe filename part from an EC key."""
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input_file", help="UniProt export (TSV/CSV)")
    ap.add_argument("--sep", default=None, help="Separator (default: guessed from extension)")
    ap.add_argument(
        "--ec-col",
        default="EC number",
        help='EC column header (default: "EC number"). If missing, script falls back to --cat-col.',
    )
    ap.add_argument(
        "--cat-col",
        default="Catalytic activity",
        help='Fallback text column to extract EC from (default: "Catalytic activity")',
    )
    ap.add_argument(
        "--mode",
        choices=["first", "explode", "joined"],
        default="explode",
        help=(
            "How to handle proteins with multiple ECs: "
            "'explode' = put the row into each EC file (recommended); "
            "'first' = use only the first EC; "
            "'joined' = create a combined key like EC:2.1.1.1|2.1.1.2"
        ),
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: alongside input file, in <stem>_ec_split/)",
    )
    args = ap.parse_args()

    in_path = Path(args.input_file)
    sep = args.sep if args.sep is not None else guess_sep(in_path)

    df = pd.read_csv(in_path, sep=sep, dtype=str)

    # Choose source column for EC extraction
    if args.ec_col in df.columns:
        ec_source = df[args.ec_col].fillna("").astype(str)
    elif args.cat_col in df.columns:
        ec_source = df[args.cat_col].fillna("").astype(str)
    else:
        # create empty series if nothing exists
        ec_source = pd.Series([""] * len(df))

    df["EC_list"] = ec_source.map(extract_ec_list)

    # Create grouping key depending on mode
    if args.mode == "first":
        df["EC_key"] = df["EC_list"].map(lambda xs: xs[0] if xs else "NO_EC")
        groups = df.groupby("EC_key", dropna=False)
    elif args.mode == "joined":
        df["EC_key"] = df["EC_list"].map(lambda xs: "|".join(xs) if xs else "NO_EC")
        groups = df.groupby("EC_key", dropna=False)
    else:  # explode
        df_ex = df.copy()
        df_ex["EC_key"] = df_ex["EC_list"]
        df_ex = df_ex.explode("EC_key")
        df_ex["EC_key"] = df_ex["EC_key"].fillna("NO_EC")
        groups = df_ex.groupby("EC_key", dropna=False)

    out_dir = Path(args.out_dir) if args.out_dir else in_path.parent / f"{in_path.stem}_ec_split"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write one file per EC group
    summary_rows = []
    for ec_key, g in groups:
        ec_key_str = str(ec_key)
        safe = sanitize_filename(ec_key_str)
        out_file = out_dir / f"{in_path.stem}_EC_{safe}{in_path.suffix or '.tsv'}"
        g.drop(columns=["EC_list"], errors="ignore").to_csv(out_file, sep=sep, index=False)
        summary_rows.append({"EC_key": ec_key_str, "rows": len(g), "file": out_file.name})

    summary = pd.DataFrame(summary_rows).sort_values(["rows", "EC_key"], ascending=[False, True])
    summary_file = out_dir / f"{in_path.stem}_EC_summary.tsv"
    summary.to_csv(summary_file, sep="\t", index=False)

    print(f"Input rows: {len(df)}")
    if args.mode == "explode":
        print(f"Exploded rows (proteins with multiple EC counted multiple times): {sum(r['rows'] for r in summary_rows)}")
    print(f"Output directory: {out_dir}")
    print(f"Summary written: {summary_file}")
    print("Top 10 EC groups:")
    print(summary.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
