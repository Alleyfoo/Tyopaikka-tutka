"""IO helpers for analytics inputs."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def load_master_shortlist(path: str | Path) -> pd.DataFrame:
    """Load Shortlist sheet from master workbook."""
    return pd.read_excel(path, sheet_name="Shortlist")


def load_jobs_file(path: str | Path) -> pd.DataFrame:
    """Load all jobs (xlsx or jsonl)."""
    path = Path(path)
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".jsonl":
        return pd.read_json(path, lines=True)
    raise ValueError("Unsupported jobs file format (use xlsx/jsonl).")


def load_jobs_diff(path: str | Path) -> pd.DataFrame:
    """Load diff (new jobs) file (xlsx or jsonl)."""
    return load_jobs_file(path)


def load_stats_sheet(master_path: str | Path) -> Optional[pd.DataFrame]:
    """Load Crawl_Stats sheet if present; otherwise return None."""
    try:
        return pd.read_excel(master_path, sheet_name="Crawl_Stats")
    except Exception:
        return None
