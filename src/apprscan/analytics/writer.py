"""Excel writer for analytics outputs."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def write_analytics(
    out_path: str | Path,
    *,
    kpi_df: pd.DataFrame,
    stations_df: pd.DataFrame,
    tags_new_df: pd.DataFrame,
    tags_all_df: Optional[pd.DataFrame] = None,
    top_companies_df: Optional[pd.DataFrame] = None,
    industry_df: Optional[pd.DataFrame] = None,
) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path) as writer:
        kpi_df.to_excel(writer, index=False, sheet_name="KPI")
        stations_df.to_excel(writer, index=False, sheet_name="Stations")
        tags_new_df.to_excel(writer, index=False, sheet_name="Tags_New")
        if tags_all_df is not None:
            tags_all_df.to_excel(writer, index=False, sheet_name="Tags_All")
        if top_companies_df is not None:
            top_companies_df.to_excel(writer, index=False, sheet_name="Top_Companies")
        if industry_df is not None:
            industry_df.to_excel(writer, index=False, sheet_name="Industry_Summary")
