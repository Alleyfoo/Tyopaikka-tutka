from apprscan.scoring import score_company


def test_score_company():
    row = {"distance_km": 0.5}
    score, reasons = score_company(
        row,
        radius_km=1.0,
        industry_whitelist_hit=True,
        excluded=False,
        recruiting_active=True,
        new_jobs=1,
        tag_counts={"data": 1, "it_support": 0, "salesforce": 0, "oppisopimus": 0},
    )
    assert score == 10  # +3 loc +3 industry +2 recruiting +1 new +1 tag
    assert "loc_ok" in reasons and "industry_ok" in reasons and "recruiting_active" in reasons

    score, reasons = score_company(
        row,
        radius_km=1.0,
        industry_whitelist_hit=False,
        industry_blacklist_hit=True,
        excluded=True,
        recruiting_active=False,
        new_jobs=0,
        tag_counts={},
    )
    assert score == -12
    assert "industry_blacklist" in reasons and "excluded" in reasons
