from pathlib import Path

import pandas as pd

from apprscan.watch import generate_watch_report


def test_generate_watch_report(tmp_path: Path):
    shortlist = pd.DataFrame(
        {"business_id": ["123"], "name": ["Test Oy"], "score": [10], "distance_km": [0.5], "nearest_station": ["Asema"]}
    )
    jobs_diff = pd.DataFrame(
        {
            "company_business_id": ["123"],
            "company_name": ["Test Oy"],
            "job_title": ["Oppisopimus Dev"],
            "job_url": ["https://example.com/jobs/1"],
            "location_text": ["Helsinki"],
            "tags": [["oppisopimus"]],
        }
    )
    out = tmp_path / "watch.txt"
    generate_watch_report(shortlist, jobs_diff, out)
    text = out.read_text(encoding="utf-8")
    assert "Oppisopimus Dev" in text
    assert "score=10" in text
    assert "watch report" in text.lower()
