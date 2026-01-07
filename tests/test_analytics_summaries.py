from pathlib import Path

import pandas as pd

from apprscan.analytics.summarize import summarize_kpi, summarize_stations, summarize_tags
from apprscan.analytics.writer import write_analytics
from apprscan.analytics.summarize import summarize_top_companies, summarize_industry


def _sample_shortlist():
    return pd.DataFrame(
        {
            "business_id": ["1", "2"],
            "name": ["A", "B"],
            "nearest_station": ["Station1", "Station1"],
            "score": [10, 5],
            "distance_km": [0.5, 1.2],
            "recruiting_active": [True, False],
            "job_count_total": [3, 0],
            "job_count_new_since_last": [1, 0],
            "lat": [60.1, 60.2],
            "lon": [24.9, 25.0],
        }
    )


def _sample_diff():
    return pd.DataFrame(
        {
            "company_business_id": ["1", "1"],
            "job_title": ["Dev", "Ops"],
            "job_url": ["u1", "u2"],
            "tags": [["data", "oppisopimus"], ["it_support"]],
            "distance_km": [0.5, 0.5],
        }
    )


def test_station_summary():
    stations = summarize_stations(_sample_shortlist(), _sample_diff())
    assert "station" in stations.columns
    assert stations.loc[stations["station"] == "Station1", "companies_total"].iloc[0] == 2
    assert stations.loc[stations["station"] == "Station1", "new_jobs_total"].iloc[0] == 2


def test_tags_summary():
    tags = summarize_tags(_sample_diff(), _sample_shortlist())
    assert set(tags["tag"]) >= {"data", "oppisopimus", "it_support"}
    opp = tags.loc[tags["tag"] == "oppisopimus", "new_jobs"].iloc[0]
    assert opp == 1


def test_kpi_summary():
    kpi = summarize_kpi(_sample_diff(), _sample_shortlist(), None)
    assert int(kpi["new_jobs_total"].iloc[0]) == 2
    assert int(kpi["companies_recruiting_active"].iloc[0]) == 1


def test_writer(tmp_path: Path):
    shortlist = _sample_shortlist()
    diff = _sample_diff()
    stations = summarize_stations(shortlist, diff)
    tags = summarize_tags(diff, shortlist)
    kpi = summarize_kpi(diff, shortlist, None)
    top_companies = summarize_top_companies(shortlist, diff, diff)
    industry = summarize_industry(shortlist, diff)
    out = tmp_path / "analytics.xlsx"
    write_analytics(
        out,
        kpi_df=kpi,
        stations_df=stations,
        tags_new_df=tags,
        top_companies_df=top_companies,
        industry_df=industry,
    )
    # sheets exist
    xls = pd.ExcelFile(out)
    assert set(["KPI", "Stations", "Tags_New", "Top_Companies", "Industry_Summary"]).issubset(set(xls.sheet_names))
