#!/usr/bin/env python
"""Merge a freshly built companies CSV into the existing one — without losing
the additive columns past pipeline stages wrote into it.

``build_companies_csv.py`` projects only its fixed 13-column schema, so a
rebuild silently drops everything the later stages added (found_website,
geocode_quality, website_status, website_http, road_distance_km, llm_*).
This tool makes the manual merge a repeatable step:

- Rows are united on ``business_id`` (string-compared).
- Old-only rows pass through with ALL their columns.
- New-only rows get the preserve columns as NaN.
- Rows in both take base columns from the NEW frame (fresh PRH data wins)
  and preserve columns from the OLD frame where the old value is not null.

Every run writes ``<out>.bak`` (a copy of the pre-merge ``--old`` file) and
prints a non-null survival summary per preserve column.

Usage
-----
  python scripts/merge_companies.py --old out/companies.csv --new out/companies_new.csv --out out/companies.csv
  python scripts/merge_companies.py --old old.csv --new new.csv --out merged.csv --preserve road_distance_km,llm_is_hiring
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

# The fixed schema build_companies_csv.py writes — anything else in the old
# CSV is additive and gets preserved by default.
BUILD_SCHEMA = [
    "name", "business_id", "industry", "toi_code", "toi_description",
    "nearest_station", "distance_km", "address", "website", "status",
    "registered", "lat", "lon",
]


def default_preserve(old_cols) -> list[str]:
    """Every old column outside the build schema (i.e. the additive ones)."""
    skip = set(BUILD_SCHEMA) | {"_bid"}
    return [c for c in old_cols if c not in skip and not str(c).startswith("Unnamed")]


def merge(
    old: pd.DataFrame, new: pd.DataFrame, preserve: list[str] | None = None
) -> tuple[pd.DataFrame, dict[str, dict[str, int]]]:
    """Union old and new on business_id, preserving additive columns.

    Returns (merged_frame, survival_summary) where the summary maps each
    preserve column to its non-null count in old vs merged.
    """
    old = old.copy()
    new = new.copy()

    # Preserve list from the ORIGINAL columns — before the internal _bid key
    # is added, so it never leaks into the preserve set.
    if preserve is None:
        preserve = default_preserve(old.columns)

    old["_bid"] = old["business_id"].astype(str).str.strip()
    new["_bid"] = new["business_id"].astype(str).str.strip()
    if old["_bid"].duplicated().any():
        raise ValueError("old: business_id ei ole yksikäsitteinen")
    if new["_bid"].duplicated().any():
        raise ValueError("new: business_id ei ole yksikäsitteinen")

    old_i = old.set_index("_bid", drop=False)
    new_i = new.set_index("_bid", drop=False)

    both_ids = old_i.index.intersection(new_i.index)  # vanhan järjestyksessä
    old_only_ids = old_i.index.difference(new_i.index)
    new_only_ids = new_i.index.difference(old_i.index)

    # Rows in both: base from new (fresh PRH wins) + additive from old.
    both = new_i.loc[both_ids].drop(columns="_bid").copy()
    both_old = old_i.loc[both_ids]
    for col in old_i.columns:
        if col in ("_bid",) or col in new_i.columns:
            continue
        both[col] = both_old[col]  # additive columns that only the old frame has
    for col in preserve:
        if col in new_i.columns and col in both.columns:
            # Old value wins where it is not null; otherwise the new value.
            both[col] = both_old[col].combine_first(both[col])

    # Old-only rows pass through untouched; new-only rows lack the additive columns.
    old_only = old_i.loc[old_only_ids].drop(columns="_bid").copy()
    new_only = new_i.loc[new_only_ids].drop(columns="_bid").copy()

    merged = pd.concat([both, old_only, new_only], ignore_index=True)

    # Column order: new build schema first, then the old frame's additive columns.
    all_cols = list(new_i.columns) + [c for c in old_i.columns if c not in new_i.columns]
    all_cols = [c for c in all_cols if c != "_bid"]
    merged = merged.reindex(columns=all_cols)

    summary = {
        col: {
            "old_nonnull": int(old[col].notna().sum()) if col in old.columns else 0,
            "merged_nonnull": int(merged[col].notna().sum()) if col in merged.columns else 0,
        }
        for col in preserve
    }
    return merged, summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--old", required=True, help="Olemassa oleva companies.csv (säilytettävät lisäsarakkeet)")
    ap.add_argument("--new", required=True, help="Tuore rakennettu companies_new.csv (PRH-perusta)")
    ap.add_argument("--out", required=True, help="Yhdistetyn CSV:n polku (kirjoitetaan)")
    ap.add_argument("--preserve", default=None,
                    help="Pilkuilla erotettu säilytettävien sarakkeiden lista "
                         "(oletus: kaikki lisätyt sarakkeet)")
    args = ap.parse_args()

    old = pd.read_csv(args.old, encoding="utf-8-sig")
    new = pd.read_csv(args.new, encoding="utf-8-sig")
    preserve = [c.strip() for c in args.preserve.split(",")] if args.preserve else None
    for col in preserve or []:
        if col not in old.columns:
            raise SystemExit(f"Virhe: saraketta {col!r} ei ole vanhassa CSV:ssä")

    merged, summary = merge(old, new, preserve)

    print(f"Vanha: {len(old)} riviä · Uusi: {len(new)} riviä · Yhdistetty: {len(merged)} riviä")
    print("\nLisäsarakkeiden säilyminen (ei-tyhjät arvot):")
    for col, counts in summary.items():
        flag = "OK " if counts["merged_nonnull"] >= counts["old_nonnull"] else "HUKKA"
        print(f"  [{flag}] {col:24s} vanha {counts['old_nonnull']:6d} -> yhdistetty {counts['merged_nonnull']:6d}")

    # Varmuuskopio ennen kirjoitusta — aina.
    bak = str(args.out) + ".bak"
    shutil.copy2(args.old, bak)
    merged.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"\nKirjoitettiin {args.out} (varmuuskopio: {bak})")


if __name__ == "__main__":
    main()