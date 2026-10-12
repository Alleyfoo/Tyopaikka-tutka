#!/usr/bin/env python
"""Build the employer LLM-analysis queue through the jev decision gate.

Unlike lead_targets.py (sales leads), this ranks companies as *employers*
using apprscan.employer_fit, then runs the jev gate (apprscan.jev_gate) to
decide which ones are worth the expensive Ollama website analysis:

  A = analysoi nyt  (live site, short commute, hiring signal or fitting industry)
  B = myöhemmin     (live site, mediocre fit)
  C = ohita         (no live site, or no fit at all)

Tier A (top ``--top`` by e_fit, plus any deep-check promotions) is written to
``out/employer_targets.csv`` for ``analyze_websites.py --ids-file``. The sales
lead queue (lead_targets.py) is untouched — this is a parallel queue.

Usage
-----
  python scripts/employer_targets.py --top 100
  python scripts/employer_targets.py --top 100 --deep 30            # heuristiikkatarkistus
  python scripts/employer_targets.py --top 100 --deep 30 --jev-model ornith-1.5:9b
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from apprscan import jev_gate  # noqa: E402
from apprscan.employer_fit import add_employer_scores  # noqa: E402

_ROOT = Path(__file__).resolve().parents[1]
CSV_IN = _ROOT / "out" / "companies.csv"
CSV_OUT = _ROOT / "out" / "employer_targets.csv"

OUT_COLS = [
    "tier", "gate_reason", "gate_model", "name", "business_id", "toi_code",
    "toi_description", "nearest_station", "dist_km", "distance_km",
    "best_website", "website_status", "registered",
    "e_industry", "e_hiring", "e_commute", "e_fit",
]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=100, help="Max Tier-A companies to write")
    ap.add_argument("--status", default="live", help="Required website_status, or 'any'")
    ap.add_argument("--deep", type=int, default=0,
                    help="Tarkista N lupaavinta B-tason yritystä kotisivun heuristiikoilla")
    ap.add_argument("--jev-model", default=None,
                    help="Ollama-malli B-tason syvätarkistukseen (valinnainen)")
    ap.add_argument("--host", default=jev_gate.DEFAULT_HOST, help="Ollama-palvelimen osoite")
    ap.add_argument("--in", dest="inp", default=str(CSV_IN))
    ap.add_argument("--out", default=str(CSV_OUT))
    args = ap.parse_args()

    df = pd.read_csv(args.inp, encoding="utf-8-sig")
    df["best_website"] = df["website"].combine_first(df.get("found_website"))
    df = add_employer_scores(df)  # Mäntsälä / bus / +10 min oletukset, kuten dashboard

    has_site = df["best_website"].notna() & (df["best_website"].astype(str).str.strip() != "")
    pool = df[has_site].copy()
    if args.status != "any" and "website_status" in pool.columns:
        pool = pool[pool["website_status"] == args.status]

    pool = jev_gate.classify(pool, deep_check=args.deep, model=args.jev_model, host=args.host)

    print("Jev-portin jakauma (elävän sivun poolissa):")
    print(pool["tier"].value_counts().to_string())

    tier_a = (
        pool[pool["tier"] == "A"]
        .sort_values("e_fit", ascending=False)
        .head(args.top)
        .copy()
    )
    cols = [c for c in OUT_COLS if c in tier_a.columns]
    tier_a[cols].to_csv(args.out, index=False, encoding="utf-8-sig")

    print(f"\nWrote {len(tier_a)} Tier-A companies to {args.out}")
    if args.deep:
        promoted = pool[(pool["gate_model"] != "rules") & (pool["tier"] == "A")]
        print(f"Deep check promoted {len(promoted)} B->A")
    print(
        f"\nNext: run the Ollama analysis on these companies "
        f"(analyze_websites.py --ids-file {args.out})."
    )


if __name__ == "__main__":
    main()