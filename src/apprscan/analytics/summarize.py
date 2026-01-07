"""Summaries for analytics outputs."""

from __future__ import annotations

from typing import Iterable, Optional

import pandas as pd
from collections import Counter


def _jobs_per_company(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=int)
    key = "company_business_id" if "company_business_id" in df.columns else "business_id"
    return df.groupby(key).size()


def summarize_stations(shortlist: pd.DataFrame, diff_jobs: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if shortlist.empty:
        return pd.DataFrame(columns=["station", "companies_total", "recruiting_active_companies", "new_jobs_total",
                                     "median_score", "median_distance_km"])
    diff_counts = _jobs_per_company(diff_jobs) if diff_jobs is not None else pd.Series(dtype=int)
    rows = []
    for station, group in shortlist.groupby("nearest_station"):
        companies_total = len(group)
        recruiting_active = int(group["recruiting_active"].fillna(False).astype(bool).sum()) if "recruiting_active" in group.columns else None
        median_score = group["score"].median() if "score" in group else None
        median_distance = group["distance_km"].median() if "distance_km" in group else None
        new_jobs_total = 0
        if not diff_counts.empty:
            bids = group["business_id"].astype(str)
            new_jobs_total = int(diff_counts.reindex(bids, fill_value=0).sum())
        rows.append(
            {
                "station": station,
                "companies_total": companies_total,
                "recruiting_active_companies": recruiting_active,
                "new_jobs_total": new_jobs_total,
                "median_score": median_score,
                "median_distance_km": median_distance,
            }
        )
    return pd.DataFrame(rows).sort_values("companies_total", ascending=False)


def _extract_tags(jobs_df: pd.DataFrame) -> pd.DataFrame:
    if jobs_df is None or jobs_df.empty or "tags" not in jobs_df.columns:
        return pd.DataFrame(columns=["tag", "business_id", "distance_km"])
    records = []
    for _, row in jobs_df.iterrows():
        tags = row.get("tags")
        if not isinstance(tags, (list, tuple)):
            continue
        bid = str(row.get("company_business_id") or row.get("business_id") or "")
        dist = row.get("distance_km")
        for tag in tags:
            records.append({"tag": tag, "business_id": bid, "distance_km": dist})
    return pd.DataFrame(records)


def summarize_tags(
    jobs_df: pd.DataFrame,
    shortlist: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    tags_df = _extract_tags(jobs_df)
    if tags_df.empty:
        return pd.DataFrame(columns=["tag", "new_jobs", "unique_companies", "median_distance_km"])
    if shortlist is not None and not shortlist.empty and "business_id" in shortlist.columns and "distance_km" in shortlist.columns:
        tags_df = tags_df.merge(
            shortlist[["business_id", "distance_km"]],
            left_on="business_id",
            right_on="business_id",
            how="left",
            suffixes=("", "_master"),
        )
        tags_df["distance_km"] = tags_df["distance_km"].fillna(tags_df["distance_km_master"])
    rows = (
        tags_df.groupby("tag")
        .agg(new_jobs=("tag", "size"), unique_companies=("business_id", "nunique"), median_distance_km=("distance_km", "median"))
        .reset_index()
        .sort_values("new_jobs", ascending=False)
    )
    return rows


def summarize_kpi(
    diff_jobs: pd.DataFrame,
    shortlist: Optional[pd.DataFrame] = None,
    stats: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    new_jobs_total = len(diff_jobs) if diff_jobs is not None else 0
    recruiting_active = (
        int(shortlist["recruiting_active"].fillna(False).astype(bool).sum())
        if shortlist is not None and "recruiting_active" in shortlist.columns
        else None
    )
    domains_crawled = domains_with_jobs = top_skips = None
    if stats is not None and not stats.empty:
        domains_crawled = len(stats)
        domains_with_jobs = int((stats["jobs_found"] > 0).sum()) if "jobs_found" in stats else None
        if "status" in stats:
            counts = stats[stats["status"] != "ok"]["status"].value_counts().head(3)
            top_skips = ";".join(f"{name}:{cnt}" for name, cnt in counts.items()) if not counts.empty else None
    return pd.DataFrame(
        [
            {
                "new_jobs_total": new_jobs_total,
                "companies_recruiting_active": recruiting_active,
                "domains_crawled": domains_crawled,
                "domains_with_jobs": domains_with_jobs,
                "top_skip_reasons": top_skips,
            }
        ]
    )


def summarize_industry(shortlist: pd.DataFrame, diff_jobs: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if shortlist is None or shortlist.empty or "industry" not in shortlist.columns:
        return pd.DataFrame(columns=["industry", "companies_total", "recruiting_active", "new_jobs_total", "median_distance_km", "top_stations"])
    diff_counts = _jobs_per_company(diff_jobs) if diff_jobs is not None else pd.Series(dtype=int)
    rows = []
    for industry, group in shortlist.groupby("industry"):
        companies_total = len(group)
        recruiting = int(group["recruiting_active"].fillna(False).astype(bool).sum()) if "recruiting_active" in group.columns else None
        median_distance = group["distance_km"].median() if "distance_km" in group else None
        new_jobs_total = 0
        if not diff_counts.empty:
            bids = group["business_id"].astype(str)
            new_jobs_total = int(diff_counts.reindex(bids, fill_value=0).sum())
        top_station = ""
        if "nearest_station" in group.columns:
            counts = group["nearest_station"].dropna().value_counts().head(3)
            if not counts.empty:
                top_station = ", ".join(f"{name} ({cnt})" for name, cnt in counts.items())
        rows.append(
            {
                "industry": industry,
                "companies_total": companies_total,
                "recruiting_active": recruiting,
                "new_jobs_total": new_jobs_total,
                "median_distance_km": median_distance,
                "top_stations": top_station,
            }
        )
    return pd.DataFrame(rows).sort_values("companies_total", ascending=False)


def summarize_top_companies(
    shortlist: pd.DataFrame,
    diff_jobs: Optional[pd.DataFrame] = None,
    all_jobs: Optional[pd.DataFrame] = None,
    top_n: int = 50,
) -> pd.DataFrame:
    if shortlist is None or shortlist.empty:
        return pd.DataFrame(
            columns=["business_id", "name", "score", "distance_km", "station", "recruiting_active", "new_jobs_count", "top_tags"]
        )
    diff_jobs = diff_jobs if diff_jobs is not None else pd.DataFrame()
    all_jobs = all_jobs if all_jobs is not None else diff_jobs

    def tag_counts_for(bid: str) -> str:
        if all_jobs is None or all_jobs.empty:
            return ""
        subset = all_jobs[all_jobs.get("company_business_id", "") == bid]
        counter = {}
        for tags in subset.get("tags", []):
            if not isinstance(tags, (list, tuple)):
                continue
            for t in tags:
                counter[t] = counter.get(t, 0) + 1
        top = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:3]
        return ";".join(f"{k}({v})" for k, v in top)

    new_counts = {}
    if diff_jobs is not None and not diff_jobs.empty and "company_business_id" in diff_jobs.columns:
        new_counts = diff_jobs["company_business_id"].value_counts().to_dict()

    rows = []
    for _, r in shortlist.iterrows():
        bid = str(r.get("business_id") or "")
        rows.append(
            {
                "business_id": bid,
                "name": r.get("name"),
                "score": r.get("score"),
                "distance_km": r.get("distance_km"),
                "station": r.get("nearest_station"),
                "recruiting_active": r.get("recruiting_active"),
                "new_jobs_count": new_counts.get(bid, 0),
                "top_tags": tag_counts_for(bid),
            }
        )
    df = pd.DataFrame(rows)
    df = df.sort_values(["new_jobs_count", "score"], ascending=[False, False]).head(top_n)
    return df
