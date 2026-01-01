#!/usr/bin/env python3
"""
Split MT2.tsv into separate TSV files (O_MT, N_MT, C_MT, S_MT, ...) using MT_grouped.tsv as the EC->group key.

Inputs:
  1) MT_grouped.tsv : one EC per line with its group (O_MT, N_MT, C_MT, ...)
     Supported formats:
       A) Sectioned format (from earlier script):
            # O_MT (..)
            ec   name   status
            2.1.1.1   ...
          (and similarly for other groups)
       B) Simple table format with columns containing EC and group, e.g.:
            ec   group
            2.1.1.1   O_MT

  2) MT2.tsv : your original dataset. It must contain an EC column (default: "EC number").
     If the EC values are embedded in text (e.g., "EC:2.1.1.1|2.1.1.2"), the script extracts ECs with regex.

Outputs:
  - <out-dir>/MT2_O_MT.tsv, MT2_N_MT.tsv, ... plus MT2_UNKNOWN.tsv for unmapped ECs.

Run:
  python split_mt2_by_group.py MT_grouped.tsv MT2.tsv
  python split_mt2_by_group.py MT_grouped.tsv MT2.tsv --ec-col "EC number" --out-dir MT2_split
"""

import argparse
import re
from collections import defaultdict
from pathlib import Path

import pandas as pd

EC_REGEX = re.compile(r"\b\d+\.\d+\.\d+\.(?:\d+|-)\b")


def parse_grouped_sectioned(path: Path) -> dict[str, str]:
    """Parse sectioned MT_grouped.tsv (# O_MT blocks). Returns ec -> group."""
    ec_to_group: dict[str, str] = {}
    current_group = None

    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.startswith("#"):
            m = re.match(r"#\s*([A-Za-z0-9_-]+)", line)
            current_group = m.group(1) if m else None
            continue

        if line.lower().startswith("ec\t"):
            continue

        if not current_group:
            continue

        ec = line.split("\t", 1)[0].strip()
        if EC_REGEX.fullmatch(ec):
            ec_to_group.setdefault(ec, current_group)

    return ec_to_group


def parse_grouped_table(path: Path) -> dict[str, str]:
    """
    Parse MT_grouped.tsv as a normal table with columns.
    Tries to find columns that look like EC and group.
    """
    df = pd.read_csv(path, sep="\t", dtype=str)

    # guess EC column
    ec_candidates = [c for c in df.columns if c.strip().lower() in {"ec", "ec_number", "ec number"}]
    if not ec_candidates:
        # fallback: any column containing "ec"
        ec_candidates = [c for c in df.columns if "ec" in c.strip().lower()]
    if not ec_candidates:
        raise RuntimeError(f"Could not find an EC column in {path.name}. Columns: {list(df.columns)}")
    ec_col = ec_candidates[0]

    # guess group column
    group_candidates = [c for c in df.columns if c.strip().lower() in {"group", "mt_group", "mt group"}]
    if not group_candidates:
        group_candidates = [c for c in df.columns if "group" in c.strip().lower()]
    if not group_candidates:
        raise RuntimeError(f"Could not find a group column in {path.name}. Columns: {list(df.columns)}")
    group_col = group_candidates[0]

    ec_to_group = {}
    for _, row in df.iterrows():
        ec = str(row.get(ec_col, "")).strip()
        group = str(row.get(group_col, "")).strip()
        if EC_REGEX.fullmatch(ec) and group:
            ec_to_group.setdefault(ec, group)

    return ec_to_group


def load_ec_to_group(path: Path) -> dict[str, str]:
    """
    Try sectioned format first; if it yields 0, try table format.
    """
    ec_to_group = parse_grouped_sectioned(path)
    if ec_to_group:
        return ec_to_group
    return parse_grouped_table(path)


def decide_row_group(ec_list: list[str], ec_to_group: dict[str, str]) -> str:
    """
    Decide which group a row belongs to based on its EC list.

    Rules:
      - no EC found -> NO_EC
      - all mapped and all in same group -> that group
      - some mapped but different groups -> MULTIPLE
      - none mapped -> UNKNOWN
      - mix of mapped+unmapped -> MIXED
    """
    if not ec_list:
        return "NO_EC"

    groups = []
    unknown = 0
    for ec in ec_list:
        g = ec_to_group.get(ec)
        if g is None:
            unknown += 1
        else:
            groups.append(g)

    if not groups and unknown:
        return "UNKNOWN"
    if groups and unknown:
        return "MIXED"

    gs = set(groups)
    if len(gs) == 1:
        return next(iter(gs))
    return "MULTIPLE"


def sanitize_filename(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mt_grouped_tsv", help="MT_grouped.tsv (EC -> group key)")
    ap.add_argument("mt2_tsv", help="MT2.tsv (original dataset)")
    ap.add_argument(
        "--ec-col",
        default="EC number",
        help='Column name in MT2.tsv containing EC info (default: "EC number"). '
             "If your ECs are in a different column, set this.",
    )
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: alongside MT2.tsv, named <MT2_stem>_by_group/)",
    )
    ap.add_argument(
        "--write-empty",
        action="store_true",
        help="Also write empty TSV files for groups that have 0 rows.",
    )
    args = ap.parse_args()

    key_path = Path(args.mt_grouped_tsv)
    mt2_path = Path(args.mt2_tsv)

    ec_to_group = load_ec_to_group(key_path)
    if not ec_to_group:
        raise RuntimeError("Loaded 0 EC->group mappings from MT_grouped.tsv.")

    df = pd.read_csv(mt2_path, sep="\t", dtype=str)

    if args.ec_col not in df.columns:
        raise RuntimeError(
            f"Column '{args.ec_col}' not found in {mt2_path.name}. Available columns: {list(df.columns)}"
        )

    # Extract EC numbers from MT2 EC column (handles single EC, EC:..., multiple separated by | ; , etc.)
    df["_ec_list"] = df[args.ec_col].fillna("").astype(str).apply(lambda s: EC_REGEX.findall(s))
    df["MT_group"] = df["_ec_list"].apply(lambda ecs: decide_row_group(ecs, ec_to_group))

    out_dir = Path(args.out_dir) if args.out_dir else mt2_path.parent / f"{mt2_path.stem}_by_group"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Determine all groups to write: groups in key + special buckets
    key_groups = sorted(set(ec_to_group.values()))
    special = ["MULTIPLE", "MIXED", "UNKNOWN", "NO_EC"]
    all_groups = key_groups + [g for g in special if g not in key_groups]

    # Write separate TSV per group
    written = 0
    for g in all_groups:
        sub = df[df["MT_group"] == g].drop(columns=["_ec_list"], errors="ignore")
        if len(sub) == 0 and not args.write_empty:
            continue
        out_file = out_dir / f"{mt2_path.stem}_{sanitize_filename(g)}.tsv"
        sub.to_csv(out_file, sep="\t", index=False)
        written += 1

    # Save a quick summary
    summary = (
        df["MT_group"]
        .value_counts(dropna=False)
        .rename_axis("MT_group")
        .reset_index(name="rows")
        .sort_values("rows", ascending=False)
    )
    summary_file = out_dir / f"{mt2_path.stem}_group_counts.tsv"
    summary.to_csv(summary_file, sep="\t", index=False)

    print(f"Loaded EC->group mappings: {len(ec_to_group)}")
    print(f"Input rows: {len(df)}")
    print(f"Wrote {written} group files to: {out_dir.resolve()}")
    print(f"Wrote summary: {summary_file.resolve()}")


if __name__ == "__main__":
    main()
