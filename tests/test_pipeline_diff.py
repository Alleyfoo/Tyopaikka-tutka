import pandas as pd

from apprscan.jobs.pipeline import apply_diff


def test_apply_diff_with_fingerprint(tmp_path):
    jobs = pd.DataFrame(
        {
            "job_title": ["Dev"],
            "location_text": ["Helsinki"],
            "posted_date": ["2024-01-01"],
            "company_domain": ["example.com"],
            "job_url": ["https://example.com/jobs/1"],
        }
    )
    known = tmp_path / "known.parquet"
    jobs_with_diff, new_jobs = apply_diff(jobs, known)
    assert len(new_jobs) == 1
    # Second run same job but different URL -> should not be new
    jobs2 = jobs.copy()
    jobs2["job_url"] = ["https://example.com/jobs/renamed"]
    jobs_with_diff2, new_jobs2 = apply_diff(jobs2, known)
    assert len(new_jobs2) == 0
