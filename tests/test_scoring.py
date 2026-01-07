from apprscan.scoring import score_company


def test_score_company():
    row = {"distance_km": 0.5}
    score, reasons = score_company(row, radius_km=1.0, industry_whitelist_hit=True, excluded=False)
    assert score == 6  # +3 loc +3 industry
    assert "loc_ok" in reasons and "industry_ok" in reasons

    score, reasons = score_company(
        row, radius_km=1.0, industry_whitelist_hit=False, industry_blacklist_hit=True, excluded=True
    )
    assert score == -12
    assert "industry_blacklist" in reasons and "excluded" in reasons
