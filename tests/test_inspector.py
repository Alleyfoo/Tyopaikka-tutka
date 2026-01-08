import pandas as pd

from apprscan.filters_view import FilterOptions
from apprscan.inspector import explain_company, select_company_jobs, get_prev_next


def test_explain_company_city_normalization():
    row = {"business_id": "1", "name": "Test Oy", "city": "Mäntsälä"}
    opts = FilterOptions(cities=["Mantsala"])
    result = explain_company(row, opts)
    assert result["passes"] is True


def test_select_company_jobs():
    jobs = pd.DataFrame(
        [
            {"company_business_id": "1", "job_title": "Dev", "job_url": "a"},
            {"company_business_id": "2", "job_title": "Ops", "job_url": "b"},
        ]
    )
    subset = select_company_jobs("1", jobs)
    assert len(subset) == 1
    assert subset.iloc[0]["job_title"] == "Dev"


def test_get_prev_next():
    ids = ["a", "b", "c"]
    prev_bid, next_bid = get_prev_next(ids, "b")
    assert prev_bid == "a"
    assert next_bid == "c"
