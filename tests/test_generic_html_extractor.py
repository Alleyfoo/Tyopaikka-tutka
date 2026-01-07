from requests import Response

from apprscan.jobs.extract.generic_html import discover_job_links, extract_jobs_generic


def test_discover_job_links():
    html = """
    <a href="/jobs/1">Apply now</a>
    <a href="/about">About</a>
    """
    urls = discover_job_links(html, "https://example.com")
    assert "https://example.com/jobs/1" in urls
    assert len(urls) == 1


class DummySession:
    def __init__(self, html):
        self.html = html

    def get(self, url, timeout=20, headers=None, allow_redirects=True):
        resp = Response()
        resp.status_code = 200
        resp.url = url
        resp._content = self.html.encode("utf-8")
        return resp


def test_extract_jobs_generic():
    list_html = '<a href="/jobs/1">Apply</a>'
    detail_html = "<h1>Support Engineer</h1><p>Helpdesk support</p>"
    session = DummySession(detail_html)
    company = {"business_id": "123", "name": "Test", "domain": "example.com"}
    jobs = extract_jobs_generic(
        session,
        list_html,
        "https://example.com/careers",
        company,
        "2024-01-01T00:00:00Z",
        rate_limit_state={},
    )
    assert len(jobs) == 1
    assert jobs[0].job_title == "Support Engineer"


def test_extract_jobs_generic_skips_listing_and_cookie():
    list_html = """
    <a href="/jobs">Jobs listing</a>
    <a href="/people/team">Team</a>
    <a href="/jobs/1">Apply</a>
    """
    consent_html = (
        "<title>Valitse evästeet</title><h1>Valitse haluamasi evästeet</h1>"
        "<p>Hyväksy tai hallinnoi evästeitä</p>"
    )
    session = DummySession(consent_html)
    company = {"business_id": "123", "name": "Test", "domain": "example.com"}
    errors: list[str] = []
    jobs = extract_jobs_generic(
        session,
        list_html,
        "https://example.com/careers",
        company,
        "2024-01-01T00:00:00Z",
        rate_limit_state={},
        errors=errors,
    )
    assert jobs == []
    assert "listing_url_skipped" in errors
    assert "cookie_consent" in errors
