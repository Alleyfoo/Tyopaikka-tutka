"""Scoring utilities."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def score_company(
    company: Dict[str, Any],
    *,
    radius_km: float = 1.0,
    industry_whitelist_hit: bool = False,
    industry_blacklist_hit: bool = False,
    excluded: bool = False,
) -> Tuple[int, str]:
    """Return (score, reasons_text)."""
    score = 0
    reasons: List[str] = []

    dist = company.get("distance_km")
    if dist is not None:
        try:
            dist_val = float(dist)
            if dist_val <= radius_km:
                score += 3
                reasons.append("loc_ok")
        except (TypeError, ValueError):
            pass

    if industry_whitelist_hit:
        score += 3
        reasons.append("industry_ok")
    if industry_blacklist_hit:
        score -= 5
        reasons.append("industry_blacklist")
    if excluded:
        score -= 10
        reasons.append("excluded")

    reasons_text = ";".join(reasons)
    return score, reasons_text
