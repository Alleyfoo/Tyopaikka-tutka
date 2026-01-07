from pathlib import Path

import pandas as pd

from apprscan.map.jobs_map import render_jobs_map


def test_render_jobs_map(tmp_path: Path):
    shortlist = pd.DataFrame(
        {
            "business_id": ["1"],
            "name": ["Company A"],
            "lat": [60.1],
            "lon": [24.9],
            "score": [5],
            "distance_km": [1.0],
            "nearest_station": ["Station1"],
            "job_count_total": [2],
            "job_count_new_since_last": [1],
            "recruiting_active": [True],
            "main_business_line": ["62010"],
            "industry": ["it"],
        }
    )
    diff = pd.DataFrame(
        {"company_business_id": ["1"], "job_title": ["Dev"], "job_url": ["https://example.com/job/1"], "tags": [["data"]]}
    )
    out = tmp_path / "jobs_map.html"
    render_jobs_map(shortlist, diff, out, mode="companies", nace_prefix=["62"])
    text = out.read_text(encoding="utf-8")
    assert "Company A" in text
    assert "example.com/job/1" in text
