"""Storage helpers for JobPosting lists."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd

from .model import JobPosting

ORDERED_COLUMNS = [
    "company_business_id",
    "company_name",
    "company_domain",
    "job_title",
    "job_url",
    "location_text",
    "employment_type",
    "posted_date",
    "description_snippet",
    "source",
    "tags",
    "crawl_ts",
]


def jobs_to_dataframe(jobs: Iterable[JobPosting]) -> pd.DataFrame:
    rows = [j.to_dict() for j in jobs]
    df = pd.DataFrame(rows)
    for col in ORDERED_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[ORDERED_COLUMNS]


def write_jobs_jsonl(jobs: Iterable[JobPosting], path: str | Path) -> None:
    df = jobs_to_dataframe(jobs)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_json(out_path, orient="records", lines=True, force_ascii=False)


def write_jobs_excel(jobs: Iterable[JobPosting], path: str | Path) -> None:
    df = jobs_to_dataframe(jobs)
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_path, index=False)


def write_jobs_outputs(jobs_df: pd.DataFrame, stats_df: pd.DataFrame, out_dir: str | Path) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs_path = out_dir / "jobs.xlsx"
    jsonl_path = out_dir / "jobs.jsonl"
    stats_path = out_dir / "crawl_stats.xlsx"
    jobs_df.to_excel(jobs_path, index=False)
    jobs_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
    stats_df.to_excel(stats_path, index=False)


def write_master_workbook(
    out_path: str | Path,
    *,
    shortlist: pd.DataFrame | None = None,
    excluded: pd.DataFrame | None = None,
    jobs_all: pd.DataFrame,
    jobs_new: pd.DataFrame,
    crawl_stats: pd.DataFrame,
    activity: pd.DataFrame | None = None,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path) as writer:
        if shortlist is not None:
            shortlist.to_excel(writer, index=False, sheet_name="Shortlist")
        if excluded is not None:
            excluded.to_excel(writer, index=False, sheet_name="Excluded")
        jobs_all.to_excel(writer, index=False, sheet_name="Jobs_All")
        jobs_new.to_excel(writer, index=False, sheet_name="Jobs_New")
        crawl_stats.to_excel(writer, index=False, sheet_name="Crawl_Stats")
        if activity is not None:
            activity.to_excel(writer, index=False, sheet_name="Company_Activity")
