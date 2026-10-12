#!/usr/bin/env python
"""Watch labeled companies for job-opening changes.

Reads the feedback store (kiinnostaa / hakenee), re-fetches those
companies' homepages and discovered careers pages, and writes
out/watchlist_alerts.csv describing what changed since the last run
(new job signal, new careers page, changed page content, fetch failure).

Also writes discovered careers URLs back into out/companies.csv as an
additive ``careers_url`` column (existing values are never overwritten).

Snapshots live in data/watchlist_snapshots.sqlite — one current row per
(business_id, url). Delete that file to reset the baseline.

Usage:
  python scripts/watchlist.py                 # watch all labeled companies
  python scripts/watchlist.py --limit 5      # first 5 only
  python scripts/watchlist.py --no-write-csv  # don't touch companies.csv
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from hashlib import sha256
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pandas as pd
import requests

from apprscan.domains_discovery import ATS_PATTERNS, _clean_domain, contains_job_signal
from apprscan.feedback import connect as feedback_connect, load as feedback_load

_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_COMPANIES = _ROOT / "out" / "companies.csv"
DEFAULT_DB = _ROOT / "data" / "watchlist_snapshots.sqlite"
DEFAULT_OUT = _ROOT / "out" / "watchlist_alerts.csv"

WATCH_LABELS = ("kiinnostaa", "hakenee")
FETCH_TIMEOUT = 15
SLEEP_S = 1.0
USER_AGENT = "TyopaikkaSkanneri-watchlist/1.0"

# Strong careers hints only. "ura" and "join" are deliberately excluded:
# they match unrelated English words and caused false positives during
# homepage probing (see ROADMAP.md 2.1).
STRONG_CAREER_HINTS = [
    "tyopaikat",
    "työpaikat",
    "tyopaikka",
    "työpaikka",
    "avoimet",
    "careers",
    "career",
    "jobs",
    "rekry",
]

# Hosts whose page text churns on every fetch (bot checks, A/B tests).
# Content-diff alerts from these are noise, so only job-signal changes count.
NOISY_HOSTS = ("linkedin.com",)


# ── Pure helpers (tested in tests/test_watchlist.py) ──────────────────────────


def text_sha256(html: str) -> str:
    """Hash the visible text of a page, so markup churn does not count as change."""
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip().lower()
    return sha256(text.encode("utf-8")).hexdigest()


def find_careers_links(html: str, base_url: str) -> list[str]:
    """Return absolute careers-page URLs found on a page.

    Matches only STRONG_CAREER_HINTS in hrefs and link texts, plus any
    ATS-hosted job board links (Greenhouse/Lever/Recruitee/...).
    """
    from urllib.parse import urljoin

    from bs4 import BeautifulSoup

    found: list[str] = []
    for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
        href = str(a["href"])
        text = (a.get_text(" ", strip=True) or "").lower()
        target = href.lower()
        strong = any(h in target for h in STRONG_CAREER_HINTS) or any(
            h in text for h in STRONG_CAREER_HINTS
        )
        ats = any(re.search(p, href) for p in ATS_PATTERNS.values())
        is_anchor_or_contact = href.startswith(("#", "mailto:", "tel:"))
        if (strong or ats) and not is_anchor_or_contact:
            url = urljoin(base_url, href)
            if url not in found:
                found.append(url)
    return found


def diff_snapshots(old: dict | None, new: dict, *, url: str = "") -> list[dict]:
    """Compare one company's old and new snapshot rows; return alert dicts.

    old is the previous snapshot row (or None on first run). Both dicts
    carry: content_sha256, job_signal, careers_urls (list). For URLs on a
    NOISY_HOSTS domain, content_changed is suppressed — those pages churn
    their text on every fetch and would raise a false alert every run.
    """
    alerts: list[dict] = []
    if old is None:
        alerts.append({"alert": "baseline", "detail": "first snapshot — baseline set"})
        return alerts
    if bool(new["job_signal"]) and not bool(old["job_signal"]):
        alerts.append({"alert": "job_signal_new", "detail": "job signal appeared on the page"})
    if bool(old["job_signal"]) and not bool(new["job_signal"]):
        alerts.append({"alert": "job_signal_lost", "detail": "job signal disappeared"})
    new_urls = [u for u in new["careers_urls"] if u not in old["careers_urls"]]
    for u in new_urls:
        alerts.append({"alert": "careers_url_new", "detail": f"new careers page: {u}"})
    noisy = any(host in url.lower() for host in NOISY_HOSTS)
    if old["content_sha256"] != new["content_sha256"] and not noisy:
        alerts.append({"alert": "content_changed", "detail": "page text changed since last check"})
    return alerts


# ── Snapshot store ────────────────────────────────────────────────────────────


def _connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            business_id   TEXT,
            url           TEXT,
            content_sha256 TEXT,
            job_signal    INTEGER,
            careers_url   TEXT,
            fetched_at    TEXT,
            PRIMARY KEY (business_id, url)
        )
        """
    )
    conn.commit()
    return conn


def _snapshot_get(conn: sqlite3.Connection, business_id: str, url: str) -> dict | None:
    row = conn.execute(
        "SELECT content_sha256, job_signal, careers_url, fetched_at FROM snapshots"
        " WHERE business_id = ? AND url = ?",
        (business_id, url),
    ).fetchone()
    if row is None:
        return None
    return {
        "content_sha256": row[0],
        "job_signal": bool(row[1]),
        "careers_urls": [u for u in (row[2] or "").split("; ") if u],
        "fetched_at": row[3],
    }


def _snapshot_set(
    conn: sqlite3.Connection, business_id: str, url: str, snap: dict
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO snapshots"
        " (business_id, url, content_sha256, job_signal, careers_url, fetched_at)"
        " VALUES (?, ?, ?, ?, ?, datetime('now'))",
        (
            business_id,
            url,
            snap["content_sha256"],
            int(snap["job_signal"]),
            "; ".join(snap["careers_urls"]),
        ),
    )
    conn.commit()


# ── Fetching ─────────────────────────────────────────────────────────────────


def _fetch(url: str) -> str | None:
    try:
        resp = requests.get(
            url,
            timeout=FETCH_TIMEOUT,
            allow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code >= 400:
            return None
        return resp.text
    except requests.RequestException:
        return None


def _ensure_scheme(url: str) -> str:
    return url if url.startswith(("http://", "https://")) else "https://" + url


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--companies", default=str(DEFAULT_COMPANIES))
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--limit", type=int, default=None, help="max companies")
    parser.add_argument(
        "--no-write-csv",
        action="store_true",
        help="don't write careers_url back into companies.csv",
    )
    args = parser.parse_args()

    # ── Watchlist: labeled companies with a website ──────────────────────────
    fb = feedback_connect()
    labels = feedback_load(fb)
    fb.close()
    watch_ids = set(
        labels.loc[labels["label"].isin(WATCH_LABELS), "business_id"].astype(str)
    )
    print(f"Watchlist: {len(watch_ids)} companies labeled {WATCH_LABELS}")

    companies = pd.read_csv(args.companies, dtype={"business_id": str})
    pool = companies[companies["business_id"].isin(watch_ids)].copy()
    if "website" in companies.columns and "found_website" in companies.columns:
        pool["site"] = pool["website"].combine_first(pool["found_website"])
    elif "website" in pool.columns:
        pool["site"] = pool["website"]
    elif "found_website" in pool.columns:
        pool["site"] = pool["found_website"]
    else:
        pool["site"] = ""
    pool["site"] = pool["site"].map(_clean_domain)
    pool = pool[pool["site"] != ""]
    if args.limit:
        pool = pool.head(args.limit)
    print(f"With a usable website: {len(pool)}")

    # ── Fetch + diff ─────────────────────────────────────────────────────────
    conn = _connect(Path(args.db))
    alert_rows: list[dict] = []
    careers_by_bid: dict[str, list[str]] = {}

    for _, row in pool.iterrows():
        bid = str(row["business_id"])
        site = str(row["site"])
        home_url = _ensure_scheme(site)
        print(f"  {row['name']}: {home_url} ...", flush=True)

        html = _fetch(home_url)
        if html is None:
            old = _snapshot_get(conn, bid, home_url)
            if old is not None:
                alert_rows.append(
                    {
                        "business_id": bid,
                        "name": row["name"],
                        "url": home_url,
                        "alert": "fetch_failed",
                        "detail": "site unreachable (was reachable last check)",
                    }
                )
            else:
                alert_rows.append(
                    {
                        "business_id": bid,
                        "name": row["name"],
                        "url": home_url,
                        "alert": "fetch_failed",
                        "detail": "site unreachable on first check",
                    }
                )
            continue

        careers_urls = find_careers_links(html, home_url)
        new_home = {
            "content_sha256": text_sha256(html),
            "job_signal": contains_job_signal(html),
            "careers_urls": careers_urls,
        }
        old_home = _snapshot_get(conn, bid, home_url)
        for a in diff_snapshots(old_home, new_home, url=home_url):
            alert_rows.append(
                {"business_id": bid, "name": row["name"], "url": home_url, **a}
            )
        _snapshot_set(conn, bid, home_url, new_home)
        if careers_urls:
            careers_by_bid[bid] = careers_urls

        # Also snapshot the first careers page so new openings there alert.
        if careers_urls:
            care_url = careers_urls[0]
            care_html = _fetch(care_url)
            if care_html is not None:
                new_care = {
                    "content_sha256": text_sha256(care_html),
                    "job_signal": contains_job_signal(care_html),
                    "careers_urls": [],
                }
                old_care = _snapshot_get(conn, bid, care_url)
                for a in diff_snapshots(old_care, new_care, url=care_url):
                    alert_rows.append(
                        {"business_id": bid, "name": row["name"], "url": care_url, **a}
                    )
                _snapshot_set(conn, bid, care_url, new_care)

        time.sleep(SLEEP_S)

    conn.close()

    # ── Alerts out ───────────────────────────────────────────────────────────
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    alerts_df = pd.DataFrame(
        alert_rows,
        columns=["business_id", "name", "url", "alert", "detail"],
    )
    alerts_df.to_csv(out_path, index=False)
    print(f"\nAlerts ({len(alerts_df)} rows) → {out_path}")
    if not alerts_df.empty:
        print(alerts_df.to_string(index=False))

    # ── Write careers_url back into companies.csv (additive) ──────────────────
    if args.no_write_csv or not careers_by_bid:
        return
    companies["careers_url"] = companies.get(
        "careers_url", pd.Series([""] * len(companies), index=companies.index)
    )
    mask = companies["business_id"].isin(careers_by_bid)
    # never overwrite an existing value
    empty = companies["careers_url"].isna() | (companies["careers_url"].astype(str) == "")
    write = mask & empty
    companies.loc[write, "careers_url"] = companies.loc[write, "business_id"].map(
        lambda b: "; ".join(careers_by_bid[b])
    )
    companies.to_csv(args.companies, index=False)
    print(f"careers_url written into companies.csv for {int(write.sum())} companies")


if __name__ == "__main__":
    main()