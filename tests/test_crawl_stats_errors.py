from apprscan.jobs.pipeline import CrawlStats


def test_crawl_stats_errors_breakdown():
    stats = CrawlStats(
        domain="example.com",
        errors=["cookie_consent", "listing_url_skipped", "cookie_consent"],
    )
    data = stats.to_dict()
    assert data["errors_count"] == 3
    assert "cookie_consent:2" in (data["errors_top"] or "")
    assert "listing_url_skipped:1" in (data["errors_top"] or "")
    assert data["status"] == "consent_gate"
