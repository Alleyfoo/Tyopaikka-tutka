import pandas as pd

from apprscan.jobs_view import join_new_jobs_with_companies


def test_join_new_jobs_with_companies():
    jobs = pd.DataFrame(
        [
            {"company_business_id": "1", "job_title": "Dev", "job_url": "a"},
        ]
    )
    companies = pd.DataFrame(
        [
            {"business_id": "1", "name": "Test Oy", "score": 10, "nearest_station": "X"},
        ]
    )
    out = join_new_jobs_with_companies(jobs, companies)
    assert "name" in out.columns
    assert out.iloc[0]["name"] == "Test Oy"
