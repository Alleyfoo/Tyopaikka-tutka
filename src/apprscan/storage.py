"""Storage and enrichment utilities."""

from __future__ import annotations

import pandas as pd


def load_employee_enrichment(path: str):
    """Load employee enrichment CSV into dict keyed by business_id."""
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    if "businessId" in df.columns:
        df = df.rename(columns={"businessId": "business_id"})
    return {str(row["business_id"]).strip(): row for _, row in df.iterrows() if "business_id" in row}
