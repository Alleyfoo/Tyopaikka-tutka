"""Helpers for Jobs view (diff-first) in Streamlit."""

from __future__ import annotations

import pandas as pd


def join_new_jobs_with_companies(jobs_new: pd.DataFrame, companies: pd.DataFrame) -> pd.DataFrame:
    if jobs_new is None or jobs_new.empty:
        return pd.DataFrame()
    if companies is None or companies.empty:
        return jobs_new.copy()
    comp_cols = [
        c
        for c in [
            "business_id",
            "name",
            "nearest_station",
            "distance_km",
            "score",
            "industry_effective",
            "status",
        ]
        if c in companies.columns
    ]
    comp = companies[comp_cols].copy()
    if "business_id" in comp.columns:
        comp["business_id"] = comp["business_id"].astype(str)
    jobs = jobs_new.copy()
    if "company_business_id" in jobs.columns:
        jobs["company_business_id"] = jobs["company_business_id"].astype(str)
        merged = jobs.merge(
            comp,
            left_on="company_business_id",
            right_on="business_id",
            how="left",
            suffixes=("", "_company"),
        )
    else:
        merged = jobs
    return merged
