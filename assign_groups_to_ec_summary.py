#!/usr/bin/env python3
"""
Use MT_grouped.tsv (with sections like "# O_MT (...)" and repeated "ec name status" headers)
to assign each EC number from MT2_EC_summary.tsv to O_MT / N_MT / C_MT / S_MT / etc.

INPUTS
  1) MT_grouped.tsv         (your grouped file with EC numbers under O_MT, N_MT, ...)
  2) MT2_EC_summary.tsv     (your EC summary file; must contain a column with EC numbers)

OUTPUTS
  - MT2_EC_summary_with_groups.tsv
  - MT2_group_totals.tsv

Run:
  python assign_groups_to_ec_summary.py MT_grouped.tsv MT2_EC_summary.tsv

Optional:
  python assign_groups_to_ec_summary.py MT_grouped.tsv MT2_EC_summary.tsv \
    --ec-col EC_key --out-summary MT2_EC_summary_with_groups.tsv --out-totals MT2_group_totals.tsv
"""

import argparse
import re
from pathlib import Path

import pandas as pd


EC_REGEX = re.compile(r"\b\d+\.\d+\.\d+\.(?:\d+|-)\b")


def parse_mt_grouped(path: Path) -> dict[str, str]:
    """
    Parses MT_grouped.tsv produced by the earlier script that looks like:

      # O_MT (123)
      ec    name    status
      2.1.1.1   ...   OK
      ...

      # N_MT (...)
      ec    name    status
      ...

    Returns mapping: ec -> group (e.g., "O_MT", "N_MT", ...)
    """
    ec_to_group: dict[str, str] = {}
    current_group = None

    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue

        # group header
        if line.startswith("#"):
            # e.g. "# O_MT (123)"
            m = re.match(r"#\s*([A-Za-z0-9_]+)\b", line)
            current_group = m.group(1) if m else None
            continue

        # skip header lines
        if line.lower().startswith("ec\t"):
            continue

        if current_group is None:
            continue

        # take first column as EC
        parts = line.split("\t")
        if not parts:
            continue
        ec = parts[0].strip()
        if EC_REGEX.fullmatch(ec):
            # keep first assignment; if duplicates exist, first wins
            ec_to_group.setdefault(ec, current_group)

    return ec_to_group


def choose_group_for_ecs(ecs: list[str], ec_to_group: dict[str, str]) -> str:
    """
    If the summary row contains multiple ECs (like "2.1.1.1|2.1.1.2"),
    decide the group:
      - if all ECs map to the same group -> that group
      - if some map to different groups -> "MULTIPLE"
      - if none map -> "UNKNOWN"
      - if mix of known+unknown -> "MIXED"
    """
    if not ecs:
        return "NO_EC"

    groups = []
    unknown = 0
    for ec in ecs:
        g = ec_to_group.get(ec)
        if g is None:
            unknown += 1
        else:
            groups.append(g)

    groups_set = set(groups)

    if not groups and unknown:
        return "UNKNOWN"
    if groups and unknown:
        # some known, some unknown
        return "MIXED"
    if len(groups_set) == 1:
        return next(iter(groups_set))
    return "MULTIPLE"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mt_grouped_tsv", help="MT_grouped.tsv (with # O_MT blocks)")
    ap.add_argument("ec_summary_tsv", help="MT2_EC_summary.tsv")
    ap.add_argument(
        "--ec-col",
        default="EC_key",
        help='Column in summary containing EC numbers (default: "EC_key"). '
             'If your file uses "EC" or similar, set it here.',
    )
    ap.add_argument(
        "--count-col",
        default="rows",
        help='Column in summary with counts (default: "rows"). If absent, totals will be based on row counts.',
    )
    ap.add_argument(
        "--out-summary",
        default=None,
        help="Output TSV with assigned groups (default: <summary_stem>_with_groups.tsv)",
    )
    ap.add_argument(
        "--out-totals",
        default=None,
        help="Output TSV with totals per group (default: <summary_stem>_group_totals.tsv)",
    )
    args = ap.parse_args()

    mt_path = Path(args.mt_grouped_tsv)
    summary_path = Path(args.ec_summary_tsv)

    ec_to_group = parse_mt_grouped(mt_path)
    if not ec_to_group:
        raise RuntimeError(
            "Parsed 0 EC->group mappings from MT_grouped.tsv. "
            "Check that it contains lines like '# O_MT (...)' and then EC numbers in the first column."
        )

    df = pd.read_csv(summary_path, sep="\t", dtype=str)

    if args.ec_col not in df.columns:
        raise RuntimeError(
            f"Column '{args.ec_col}' not found in {summary_path.name}. "
            f"Available columns: {list(df.columns)}"
        )

    # Extract ECs from the EC column (handles '2.1.1.1', 'EC:2.1.1.1|2.1.1.2', etc.)
    df["_ecs"] = df[args.ec_col].fillna("").astype(str).apply(lambda s: EC_REGEX.findall(s))
    df["MT_group"] = df["_ecs"].apply(lambda ecs: choose_group_for_ecs(ecs, ec_to_group))

    # Output 1: annotated summary
    out_summary = Path(args.out_summary) if args.out_summary else summary_path.with_name(f"{summary_path.stem}_with_groups.tsv")
    df.drop(columns=["_ecs"], errors="ignore").to_csv(out_summary, sep="\t", index=False)

    # Output 2: totals per group
    # If count column exists and is numeric-ish -> sum it; otherwise just count rows
    if args.count_col in df.columns:
        counts = pd.to_numeric(df[args.count_col], errors="coerce")
        df["_count_num"] = counts.fillna(0).astype(int)
        totals = (
            df.groupby("MT_group", dropna=False)
              .agg(
                  total_rows_in_summary=("MT_group", "size"),
                  total_count=( "_count_num", "sum"),
                  distinct_ec_keys=(args.ec_col, pd.Series.nunique),
              )
              .reset_index()
              .sort_values(["total_count", "total_rows_in_summary"], ascending=False)
        )
        df.drop(columns=["_count_num"], inplace=True, errors="ignore")
    else:
        totals = (
            df.groupby("MT_group", dropna=False)
              .agg(
                  total_rows_in_summary=("MT_group", "size"),
                  distinct_ec_keys=(args.ec_col, pd.Series.nunique),
              )
              .reset_index()
              .sort_values(["total_rows_in_summary"], ascending=False)
        )

    out_totals = Path(args.out_totals) if args.out_totals else summary_path.with_name(f"{summary_path.stem}_group_totals.tsv")
    totals.to_csv(out_totals, sep="\t", index=False)

    print(f"Loaded EC->group mappings: {len(ec_to_group)}")
    print(f"Wrote: {out_summary}")
    print(f"Wrote: {out_totals}")


if __name__ == "__main__":
    main()
