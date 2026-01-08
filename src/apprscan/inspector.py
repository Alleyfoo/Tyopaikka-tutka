"""Helpers for inspector view logic (no Streamlit dependencies)."""

from __future__ import annotations

from typing import Dict, Iterable, Tuple

import pandas as pd

from .filters import is_housing_company
from .filters_view import FilterOptions


def explain_company(row: Dict[str, object] | pd.Series, opts: FilterOptions) -> Dict[str, object]:
    """Explain why a company passes/fails current filters."""
    r = row.to_dict() if isinstance(row, pd.Series) else row
    reasons: list[str] = []
    fails: list[str] = []

    name = str(r.get("name") or "")
    if not opts.include_housing and is_housing_company(name):
        fails.append("housing_name")

    if not opts.include_hidden and r.get("hide_flag") is True:
        fails.append("hidden")

    if not opts.include_excluded and (r.get("excluded_reason") or ""):
        fails.append("excluded_reason")

    if opts.statuses:
        status = str(r.get("status") or "")
        if status not in opts.statuses:
            fails.append("status")
        else:
            reasons.append(f"status:{status}")

    if opts.industries:
        ind = str(r.get("industry_effective") or "")
        if ind not in opts.industries:
            fails.append("industry")
        else:
            reasons.append(f"industry:{ind}")

    if opts.cities:
        city_val = str(r.get("city") or r.get("addresses.0.city") or r.get("_source_city") or r.get("domicile") or "")
        city_norm = city_val.strip().lower().translate(str.maketrans({"ä": "a", "ö": "o", "å": "a"}))
        target = {c.strip().lower().translate(str.maketrans({"ä": "a", "ö": "o", "å": "a"})) for c in opts.cities}
        if city_norm not in target:
            fails.append("city")
        else:
            reasons.append(f"city:{city_val}")

    if opts.min_score is not None and pd.notna(r.get("score")):
        score = float(r.get("score"))
        if score < float(opts.min_score):
            fails.append("min_score")
        else:
            reasons.append("min_score")

    if opts.max_distance_km is not None and pd.notna(r.get("distance_km")):
        dist = float(r.get("distance_km"))
        if dist > float(opts.max_distance_km):
            fails.append("max_distance_km")
        else:
            reasons.append("max_distance_km")

    if opts.stations:
        station = str(r.get("nearest_station") or "")
        if station not in opts.stations:
            fails.append("station")
        else:
            reasons.append(f"station:{station}")

    if opts.only_recruiting:
        if not bool(r.get("recruiting_active")):
            fails.append("only_recruiting")
        else:
            reasons.append("recruiting_active")

    if opts.include_tags:
        tags = r.get("tags_effective") or []
        tags_set = set(tags) if isinstance(tags, list) else set()
        if not tags_set.intersection(set(opts.include_tags)):
            fails.append("include_tags")
        else:
            reasons.append("include_tags")

    if opts.exclude_tags:
        tags = r.get("tags_effective") or []
        tags_set = set(tags) if isinstance(tags, list) else set()
        if tags_set.intersection(set(opts.exclude_tags)):
            fails.append("exclude_tags")

    if opts.search:
        needle = opts.search.lower().strip()
        hay = " ".join(
            str(r.get(k, "") or "").lower() for k in ["name", "business_id", "website.url", "note"]
        )
        if needle and needle not in hay:
            fails.append("search")

    return {"passes": len(fails) == 0, "reasons": reasons, "fails": fails}


def select_company_jobs(
    business_id: str, jobs_df: pd.DataFrame
) -> pd.DataFrame:
    if jobs_df is None or jobs_df.empty:
        return pd.DataFrame()
    bid = str(business_id or "")
    col = "company_business_id" if "company_business_id" in jobs_df.columns else "business_id"
    return jobs_df[jobs_df[col].astype(str) == bid].copy()


def get_prev_next(view_ids: Iterable[str], current_bid: str) -> Tuple[str | None, str | None]:
    ids = [str(x) for x in view_ids]
    if not ids or current_bid not in ids:
        return None, None
    idx = ids.index(current_bid)
    prev_bid = ids[idx - 1] if idx > 0 else None
    next_bid = ids[idx + 1] if idx < len(ids) - 1 else None
    return prev_bid, next_bid
