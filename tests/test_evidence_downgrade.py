from apprscan.server import service


def test_single_url_downgrades_yes():
    scan_cfg = service.load_scan_config()
    package = service.build_company_package(
        run_id="run",
        maps_url="https://www.google.com/maps",
        place_id=None,
        website_url="https://example.com",
        domain="example.com",
        website_source="places",
        resolver_notes="",
        scan_config=scan_cfg,
        scan_result={
            "signal": "yes",
            "confidence": 0.9,
            "evidence": "job_signal_keywords",
            "evidence_snippets": ["we are hiring", "open positions"],
            "evidence_urls": ["https://example.com/careers"],
        },
        checked_urls=["https://example.com/careers"],
        errors=[],
        skipped_reasons=[],
        pages_fetched=1,
        note="",
        tags=[],
        pipeline_status="ok",
        degraded_reason="none",
        next_action="",
    )
    assert package["hiring"]["status"] == "maybe"


def test_two_urls_keeps_yes():
    scan_cfg = service.load_scan_config()
    package = service.build_company_package(
        run_id="run",
        maps_url="https://www.google.com/maps",
        place_id=None,
        website_url="https://example.com",
        domain="example.com",
        website_source="places",
        resolver_notes="",
        scan_config=scan_cfg,
        scan_result={
            "signal": "yes",
            "confidence": 0.9,
            "evidence": "job_signal_keywords",
            "evidence_snippets": ["we are hiring", "open positions"],
            "evidence_urls": ["https://example.com/careers", "https://example.com/jobs"],
        },
        checked_urls=["https://example.com/careers", "https://example.com/jobs"],
        errors=[],
        skipped_reasons=[],
        pages_fetched=1,
        note="",
        tags=[],
        pipeline_status="ok",
        degraded_reason="none",
        next_action="",
    )
    assert package["hiring"]["status"] == "yes"
