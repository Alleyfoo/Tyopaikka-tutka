import pandas as pd

from apprscan.jobs import pipeline
from apprscan.jobs.constants import ROBOTS_DISALLOW_ALL, ROBOTS_DISALLOW_URL
from apprscan.jobs.model import JobPosting
from apprscan.jobs.storage import jobs_to_dataframe


class FakeRobotsChecker:
    def __init__(self, rules):
        self.rules = rules

    def can_fetch_detail(self, url: str):
        # rules: dict prefix/url -> (bool, rule)
        for key, val in sorted(self.rules.items(), key=lambda kv: len(kv[0]), reverse=True):
            if url.startswith(key):
                return val
        return True, None


def _fake_fetch_url(reason=None):
    def _inner(session, url, **kwargs):
        if reason:
            return None, reason
        # return dummy response-like with html attr
        class Resp:
            def __init__(self, url):
                self.status = 200
                self.final_url = url
                self.html = "<html></html>"
                self.headers = {}
        return Resp(url), None

    return _inner


def _run_crawl_with_robots(rules, fetch_reason=None):
    company = {"business_id": "1", "name": "Test", "domain": "example.com"}

    fake_checker = FakeRobotsChecker(rules)
    orig_checker = pipeline.RobotsChecker
    orig_fetch = pipeline.fetch_url
    pipeline.RobotsChecker = lambda: fake_checker  # type: ignore
    pipeline.fetch_url = _fake_fetch_url(fetch_reason)  # type: ignore

    try:
        jobs, stats = pipeline.crawl_domain(
            company,
            company["domain"],
            max_pages=5,
            req_per_second=1.0,
            rate_limit_state={},
            debug_html_dir=None,
            session=None,  # not used by fake fetcher
            crawl_ts="ts",
            tag_rules=None,
        )
        jobs_df = jobs_to_dataframe(jobs)
        stats_df = pd.DataFrame([stats.to_dict()])
    finally:
        pipeline.RobotsChecker = orig_checker
        pipeline.fetch_url = orig_fetch
    return jobs_df, stats_df


def test_robots_disallow_all_blocks():
    rules = {"https://example.com": (False, "Disallow: /")}
    jobs, stats = _run_crawl_with_robots(rules)
    assert len(jobs) == 0
    assert stats.loc[0, "skipped_reason"] == ROBOTS_DISALLOW_ALL
    assert stats.loc[0, "first_blocked_url"] == "https://example.com"


def test_robots_disallow_url_blocks_seed():
    base = "https://example.com"
    rules = {
        base: (True, None),
        f"{base}/careers": (False, "blocked_by_robots"),
        f"{base}/careers/": (False, "blocked_by_robots"),
    }
    jobs, stats = _run_crawl_with_robots(rules)
    assert ROBOTS_DISALLOW_URL in (stats.loc[0, "skipped_reason"], stats.loc[0, "errors"])


def test_http_403_skip_reason():
    base = "https://example.com"
    rules = {base: (True, None)}
    jobs, stats = _run_crawl_with_robots(rules, fetch_reason="http_403")
    assert stats.loc[0, "jobs_found"] == 0
    assert "http_403" in (stats.loc[0, "skipped_reason"] or "") or "http_403" in (stats.loc[0, "errors"] or [])


def test_timeout_reason():
    base = "https://example.com"
    rules = {base: (True, None)}
    jobs, stats = _run_crawl_with_robots(rules, fetch_reason="timeout")
    assert stats.loc[0, "jobs_found"] == 0
    assert "timeout" in (stats.loc[0, "skipped_reason"] or "") or "timeout" in (stats.loc[0, "errors"] or [])


def test_dns_reason():
    base = "https://example.com"
    rules = {base: (True, None)}
    jobs, stats = _run_crawl_with_robots(rules, fetch_reason="dns")
    assert stats.loc[0, "jobs_found"] == 0
    assert "dns" in (stats.loc[0, "skipped_reason"] or "") or "dns" in (stats.loc[0, "errors"] or [])
