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


def test_watch_fallback_name(tmp_path: Path):
    shortlist = pd.DataFrame(
        {
            "business_id": ["123"],
            "name": ["Shortlist Name"],
            "score": [5],
            "distance_km": [1.2],
        }
    )
    jobs_diff = pd.DataFrame(
        {
            "company_business_id": ["123"],
            "company_name": [float("nan")],
            "job_title": ["New Role"],
            "job_url": ["https://example.com/jobs/1"],
            "tags": [[]],
        }
    )
    out = tmp_path / "watch.txt"
    generate_watch_report(shortlist, jobs_diff, out)
    text = out.read_text(encoding="utf-8")
    assert "Shortlist Name (123)" in text


def test_watch_includes_crawl_summary(tmp_path: Path):
    shortlist = pd.DataFrame({"business_id": ["1"], "name": ["Test"], "score": [1]})
    jobs_diff = pd.DataFrame(
        {"company_business_id": ["1"], "company_name": ["Test"], "job_title": ["Role"], "job_url": ["u"]}
    )
    stats = pd.DataFrame({"domain": ["a"], "jobs_found": [0], "status": ["consent_gate"], "errors_top": ["cookie_consent:2"]})
    out = tmp_path / "watch.txt"
    generate_watch_report(shortlist, jobs_diff, out, stats=stats)
    text = out.read_text(encoding="utf-8")
    assert "Crawl coverage:" in text
    assert "consent_gate" in text
    assert "cookie_consent" in text
