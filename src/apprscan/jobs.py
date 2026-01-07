"""Jobs pipeline: crawl careers pages and normalize JobPosting rows."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import Response
from urllib.robotparser import RobotFileParser

JOB_PATHS = [
    "/careers",
    "/jobs",
    "/rekry",
    "/tyopaikat",
    "/ura",
    "/open-positions",
]

TAG_KEYWORDS = {
    "oppisopimus": ["oppisopimus", "oppisopimuskoulutus", "apprentice", "apprenticeship"],
    "internship": ["internship", "trainee", "harjoittelu"],
    "junior": ["junior", "entry-level", "entry level"],
    "data": ["data", "analytiikka", "analytics", "data engineer", "data scientist"],
    "it-support": ["it support", "helpdesk", "service desk"],
    "marketing": ["marketing", "markkinointi"],
    "salesforce": ["salesforce"],
}


@dataclass
class JobPosting:
    company_business_id: str
    company_name: str
    company_domain: str
    job_title: str
    job_url: str
    location_text: str | None
    employment_type: str | None
    posted_date: str | None
    description_snippet: str | None
    source: str
    tags: List[str]
    crawl_ts: str

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


def detect_tags(title: str, description: str | None) -> List[str]:
    text = f"{title} {description or ''}".lower()
    tags: List[str] = []
    for tag, kws in TAG_KEYWORDS.items():
        if any(kw in text for kw in kws):
            tags.append(tag)
    return tags


def parse_jsonld_jobs(html: str, base_url: str, company: Dict[str, str], crawl_ts: str) -> List[JobPosting]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[JobPosting] = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(data, dict):
            candidates = [data]
        elif isinstance(data, list):
            candidates = data
        else:
            continue
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if item.get("@type") not in ("JobPosting", ["JobPosting"]):
                continue
            title = item.get("title") or ""
            url = item.get("url") or base_url
            desc = item.get("description") or ""
            loc = None
            if isinstance(item.get("jobLocation"), dict):
                addr = item["jobLocation"].get("address", {})
                loc = addr.get("addressLocality") or addr.get("streetAddress")
            emp_type = None
            hiring = item.get("employmentType")
            if isinstance(hiring, str):
                emp_type = hiring
            posted = item.get("datePosted")
            tags = detect_tags(title, desc)
            jobs.append(
                JobPosting(
                    company_business_id=company.get("business_id", ""),
                    company_name=company.get("name", ""),
                    company_domain=company.get("domain", ""),
                    job_title=title,
                    job_url=urljoin(base_url, url),
                    location_text=loc,
                    employment_type=emp_type,
                    posted_date=posted,
                    description_snippet=(desc[:280] if desc else None),
                    source="jsonld",
                    tags=tags,
                    crawl_ts=crawl_ts,
                )
            )
    return jobs


def detect_ats(html: str, url: str) -> Optional[str]:
    text = html.lower()
    if "greenhouse.io" in text:
        return "greenhouse"
    if "lever.co" in text or "jobs.lever.co" in url:
        return "lever"
    if "teamtailor" in text:
        return "teamtailor"
    if "recruitee" in text:
        return "recruitee"
    return None


def extract_generic_links(html: str, base_url: str, company: Dict[str, str], crawl_ts: str) -> List[JobPosting]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: List[JobPosting] = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(" ", strip=True)
        if not text:
            continue
        if any(word in text.lower() for word in ["job", "position", "paikka", "role", "tehtävä", "trainee"]):
            full_url = urljoin(base_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)
            tags = detect_tags(text, None)
            jobs.append(
                JobPosting(
                    company_business_id=company.get("business_id", ""),
                    company_name=company.get("name", ""),
                    company_domain=company.get("domain", ""),
                    job_title=text,
                    job_url=full_url,
                    location_text=None,
                    employment_type=None,
                    posted_date=None,
                    description_snippet=None,
                    source="generic_html",
                    tags=tags,
                    crawl_ts=crawl_ts,
                )
            )
    return jobs


def fetch_url(session: requests.Session, url: str, timeout: float = 20.0) -> Optional[Response]:
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code >= 400:
            return None
        return resp
    except requests.RequestException:
        return None


def load_domain_mapping(path: Optional[str]) -> Dict[str, str]:
    if not path:
        return {}
    df = pd.read_csv(path)
    mapping = {}
    for _, row in df.iterrows():
        bid = str(row.get("business_id") or row.get("businessId") or "").strip()
        domain = str(row.get("domain") or "").strip()
        if bid and domain:
            mapping[bid] = domain
    return mapping


def discover_career_urls(domain: str) -> List[str]:
    base = f"https://{domain}"
    return [urljoin(base, path) for path in JOB_PATHS]


def can_fetch(url: str, cache: Dict[str, RobotFileParser]) -> bool:
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    if robots_url not in cache:
        parser = RobotFileParser()
        try:
            parser.set_url(robots_url)
            parser.read()
        except Exception:
            parser = RobotFileParser()
            parser.parse("")  # allow all
        cache[robots_url] = parser
    parser = cache[robots_url]
    return parser.can_fetch("*", url)


def crawl_domain(
    session: requests.Session,
    company: Dict[str, str],
    domain: str,
    max_pages: int,
    rate_limit_state: Dict[str, float],
    robots_cache: Dict[str, RobotFileParser],
    crawl_ts: str,
) -> Tuple[List[JobPosting], Dict[str, object]]:
    pages_fetched = 0
    jobs: List[JobPosting] = []
    errors: List[str] = []
    used_plugin: Optional[str] = None

    for url in discover_career_urls(domain):
        if pages_fetched >= max_pages:
            break
        if not can_fetch(url, robots_cache):
            errors.append(f"robots_blocked:{url}")
            continue

        last = rate_limit_state.get(domain, 0)
        sleep_for = max(0, 1.0 - (time.time() - last))
        if sleep_for > 0:
            time.sleep(sleep_for)

        resp = fetch_url(session, url)
        rate_limit_state[domain] = time.time()
        if resp is None:
            errors.append(f"fetch_failed:{url}")
            continue
        pages_fetched += 1
        html = resp.text

        jsonld_jobs = parse_jsonld_jobs(html, url, company, crawl_ts)
        if jsonld_jobs:
            jobs.extend(jsonld_jobs)
            used_plugin = "jsonld"
            continue

        plugin = detect_ats(html, url)
        if plugin:
            used_plugin = plugin
            # Placeholder: implement plugin-specific fetch in future
            continue

        jobs.extend(extract_generic_links(html, url, company, crawl_ts))

    stats = {
        "domain": domain,
        "pages_fetched": pages_fetched,
        "jobs_found": len(jobs),
        "plugin_used": used_plugin,
        "errors": ";".join(errors) if errors else None,
    }
    return jobs, stats


def crawl_jobs_for_companies(
    companies: pd.DataFrame,
    domain_map: Dict[str, str],
    max_domains: int = 300,
    max_pages_per_domain: int = 30,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    session = requests.Session()
    rate_limit_state: Dict[str, float] = {}
    robots_cache: Dict[str, RobotFileParser] = {}
    all_jobs: List[JobPosting] = []
    stats_rows = []
    crawl_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    processed = 0
    for _, row in companies.iterrows():
        if processed >= max_domains:
            break
        bid = str(row.get("business_id") or "").strip()
        domain = str(row.get("domain") or domain_map.get(bid, "")).strip()
        if not domain:
            continue
        company = {"business_id": bid, "name": row.get("name", ""), "domain": domain}
        jobs, stats = crawl_domain(
            session,
            company,
            domain,
            max_pages=max_pages_per_domain,
            rate_limit_state=rate_limit_state,
            robots_cache=robots_cache,
            crawl_ts=crawl_ts,
        )
        all_jobs.extend(jobs)
        stats_rows.append(stats)
        processed += 1

    jobs_df = pd.DataFrame([job.to_dict() for job in all_jobs])
    stats_df = pd.DataFrame(stats_rows)
    return jobs_df, stats_df


def write_jobs_outputs(jobs_df: pd.DataFrame, stats_df: pd.DataFrame, out_dir: str) -> None:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    jobs_path = out_path / "jobs.xlsx"
    jsonl_path = out_path / "jobs.jsonl"
    stats_path = out_path / "crawl_stats.xlsx"

    jobs_df.to_excel(jobs_path, index=False)
    jobs_df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
    stats_df.to_excel(stats_path, index=False)
