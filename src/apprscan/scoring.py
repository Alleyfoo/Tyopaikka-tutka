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
    recruiting_active: bool = False,
    new_jobs: int = 0,
    tag_counts: Dict[str, int] | None = None,
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
    if recruiting_active:
        score += 2
        reasons.append("recruiting_active")
    if new_jobs > 0:
        score += 1
        reasons.append("new_jobs")
    if tag_counts:
        for tag in ("data", "it_support", "salesforce", "oppisopimus"):
            if tag_counts.get(tag, 0) > 0:
                score += 1
                reasons.append(f"tag_{tag}")

    reasons_text = ";".join(reasons)
    return score, reasons_text
