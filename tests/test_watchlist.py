"""Tests for scripts/watchlist.py (loaded via importlib, merge-script pattern)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "watchlist_script", _ROOT / "scripts" / "watchlist.py"
)
watchlist = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("watchlist_script", watchlist)
_spec.loader.exec_module(watchlist)


HOME_HTML = """
<html><body>
  <a href="/palvelut">Palvelut</a>
  <a href="/tyopaikat">Työpaikat</a>
  <a href="https://boards.greenhouse.io/acme">Jobs</a>
</body></html>
"""


def test_find_careers_links_strong_and_ats():
    links = watchlist.find_careers_links(HOME_HTML, "https://acme.fi")
    assert "https://acme.fi/tyopaikat" in links
    assert "https://boards.greenhouse.io/acme" in links
    # non-careers links are not included
    assert "https://acme.fi/palvelut" not in links


def test_find_careers_links_ignores_weak_ura_signal():
    """A link merely containing 'ura' (e.g. English 'structural') must not match."""
    html = '<a href="/company/structural-design">Structural design</a>'
    links = watchlist.find_careers_links(html, "https://acme.fi")
    assert links == []


def test_text_sha256_ignores_markup_churn():
    a = "<div><p>Hello world</p></div>"
    b = "<span>Hello   world</span>"
    assert watchlist.text_sha256(a) == watchlist.text_sha256(b)
    assert watchlist.text_sha256(a) != watchlist.text_sha256("<p>Hello there</p>")


def test_diff_first_run_is_baseline():
    new = {"content_sha256": "x", "job_signal": True, "careers_urls": ["u"]}
    alerts = watchlist.diff_snapshots(None, new)
    assert [a["alert"] for a in alerts] == ["baseline"]


def test_diff_new_job_signal_and_careers_url():
    old = {"content_sha256": "x", "job_signal": False, "careers_urls": []}
    new = {"content_sha256": "x", "job_signal": True, "careers_urls": ["u"]}
    alerts = watchlist.diff_snapshots(old, new)
    kinds = [a["alert"] for a in alerts]
    assert "job_signal_new" in kinds
    assert "careers_url_new" in kinds
    assert "content_changed" not in kinds


def test_diff_lost_signal_and_content_change():
    old = {"content_sha256": "x", "job_signal": True, "careers_urls": ["u"]}
    new = {"content_sha256": "y", "job_signal": False, "careers_urls": ["u"]}
    alerts = watchlist.diff_snapshots(old, new)
    kinds = [a["alert"] for a in alerts]
    assert "job_signal_lost" in kinds
    assert "content_changed" in kinds
    assert "careers_url_new" not in kinds


def test_diff_unchanged_is_silent():
    old = {"content_sha256": "x", "job_signal": True, "careers_urls": ["u"]}
    new = {"content_sha256": "x", "job_signal": True, "careers_urls": ["u"]}
    assert watchlist.diff_snapshots(old, new) == []


def test_diff_suppresses_content_change_on_noisy_hosts():
    """LinkedIn pages churn on every fetch — content_changed must not fire there."""
    old = {"content_sha256": "x", "job_signal": False, "careers_urls": ["u"]}
    new = {"content_sha256": "y", "job_signal": False, "careers_urls": ["u"]}
    url = "https://www.linkedin.com/company/oenneo/jobs/"
    alerts = watchlist.diff_snapshots(old, new, url=url)
    assert [a["alert"] for a in alerts] == []


def test_snapshot_roundtrip(tmp_path):
    db = tmp_path / "snap.sqlite"
    conn = watchlist._connect(db)
    assert watchlist._snapshot_get(conn, "1-1", "https://x.fi") is None
    snap = {"content_sha256": "abc", "job_signal": True, "careers_urls": ["https://x.fi/jobs"]}
    watchlist._snapshot_set(conn, "1-1", "https://x.fi", snap)
    got = watchlist._snapshot_get(conn, "1-1", "https://x.fi")
    assert got["content_sha256"] == "abc"
    assert got["job_signal"] is True
    assert got["careers_urls"] == ["https://x.fi/jobs"]
    conn.close()