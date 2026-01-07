from pathlib import Path

import pandas as pd
from requests import Response

import apprscan.domains_discovery as dd


class DummyResp(Response):
    def __init__(self, url: str, status: int, html: str):
        super().__init__()
        self.status_code = status
        self.url = url
        self._content = html.encode("utf-8")


def test_ats_detection_high_confidence(monkeypatch):
    html = '<a href="https://boards.greenhouse.io/testco">Careers</a>'

    def fake_fetch(url, timeout=10.0):
        return html

    monkeypatch.setattr(dd, "_fetch", fake_fetch)
    sug = dd.suggest_for_company("1", "Test", "example.com")
    assert sug is not None
    assert "greenhouse" in sug.reason
    assert sug.suggested_base_url == "https://boards.greenhouse.io/testco"


def test_common_path_detection(monkeypatch):
    pages = {
        "https://example.com/": "",
        "https://example.com/careers": "<h1>Open positions</h1>",
    }

    def fake_fetch(url, timeout=10.0):
        return pages.get(url, None)

    monkeypatch.setattr(dd, "_fetch", fake_fetch)
    sug = dd.suggest_for_company("1", "Test", "example.com")
    assert sug is not None
    assert sug.suggested_base_url.endswith("/careers")
    assert sug.source == "common_path"


def test_suggest_domains_dataframe(monkeypatch):
    df = pd.DataFrame({"business_id": ["1"], "name": ["Test"], "domain": ["example.com"]})

    def fake_suggest_for_company(bid, name, domain):
        return dd.DomainSuggestion(
            business_id=bid,
            name=name,
            homepage_domain=domain,
            suggested_base_url="https://example.com/careers",
            source="test",
            confidence="high",
            reason="",
        )

    monkeypatch.setattr(dd, "suggest_for_company", fake_suggest_for_company)
    out = dd.suggest_domains(df, max_companies=1)
    assert "suggested_base_url" in out.columns
    assert len(out) == 1
