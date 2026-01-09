"""Domain / careers URL discovery helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

CAREER_HINTS = [
    "career",
    "careers",
    "job",
    "jobs",
    "open-position",
    "open-positions",
    "rekry",
    "rekrytointi",
    "ura",
    "tyopaikat",
    "työpaikat",
    "join",
]
COMMON_PATHS = ["/careers", "/jobs", "/open-positions", "/rekry", "/ura", "/tyopaikat"]
ATS_PATTERNS = {
    "greenhouse": r"boards\.greenhouse\.io/([^/]+)/?",
    "lever": r"jobs\.lever\.co/([^/]+)/?",
    "recruitee": r"\.recruitee\.com",
    "teamtailor": r"\.teamtailor\.com",
    "smartrecruiters": r"\.smartrecruiters\.com",
}


@dataclass
class DomainSuggestion:
    business_id: str
    name: str
    homepage_domain: str
    suggested_base_url: str
    source: str
    confidence: str
    reason: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "business_id": self.business_id,
            "name": self.name,
            "homepage_domain": self.homepage_domain,
            "suggested_base_url": self.suggested_base_url,
            "source": self.source,
            "confidence": self.confidence,
            "reason": self.reason,
        }


def _clean_domain(val: str) -> str:
    val = (val or "").strip()
    if not val or val.lower() in {"nan", "none", "null"}:
        return ""
    # strip scheme
    parsed = urlparse(val if "://" in val else f"https://{val}")
    return parsed.netloc or parsed.path


def _fetch(url: str, timeout: float = 10.0) -> Optional[str]:
    try:
        resp = requests.get(url, timeout=timeout, allow_redirects=True, headers={"User-Agent": "apprscan-domain/0.1"})
        if resp.status_code >= 400:
            return None
        return resp.text
    except requests.RequestException:
        return None


def contains_job_signal(html: str) -> bool:
    if not html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()
    if "jobposting" in html.lower():
        return True
    return any(
        h in text
        for h in [
            "open positions",
            "open roles",
            "apply",
            "tyopaikat",
            "avoin tehtava",
            "hae tahan",
            "tyApaikat",
            "avoin tehtAvA",
            "hae tAhAn",
        ]
    )


def _find_links(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = (a.get_text(" ", strip=True) or "").lower()
        if any(h in href.lower() for h in CAREER_HINTS) or any(h in text for h in CAREER_HINTS):
            links.append(urljoin(base_url, href))
    return links


def _ats_from_links(links: Iterable[str]) -> Optional[DomainSuggestion]:
    for link in links:
        for kind, pattern in ATS_PATTERNS.items():
            if re.search(pattern, link):
                return DomainSuggestion(
                    business_id="",
                    name="",
                    homepage_domain="",
                    suggested_base_url=link,
                    source="ats",
                    confidence="high",
                    reason=f"found {kind} link",
                )
    return None


def suggest_for_company(business_id: str, name: str, domain: str) -> Optional[DomainSuggestion]:
    domain_clean = _clean_domain(domain)
    if not domain_clean:
        return None

    base = f"https://{domain_clean}"
    html = _fetch(base)
    links = _find_links(html or "", base) if html else []
    ats_suggestion = _ats_from_links(links)
    if ats_suggestion:
        ats_suggestion.business_id = business_id
        ats_suggestion.name = name
        ats_suggestion.homepage_domain = domain_clean
        return ats_suggestion

    # test common paths
    for path in COMMON_PATHS:
        url = f"{base}{path}"
        page = _fetch(url)
        if page and contains_job_signal(page):
            return DomainSuggestion(
                business_id=business_id,
                name=name,
                homepage_domain=domain_clean,
                suggested_base_url=url,
                source="common_path",
                confidence="med",
                reason=f"matched common path {path}",
            )

    # links from homepage
    for link in links:
        page = _fetch(link)
        if page and contains_job_signal(page):
            return DomainSuggestion(
                business_id=business_id,
                name=name,
                homepage_domain=domain_clean,
                suggested_base_url=link,
                source="homepage_link",
                confidence="med",
                reason="homepage link with job signals",
            )
    return None


def suggest_domains(companies_df: pd.DataFrame, max_companies: int = 200) -> pd.DataFrame:
    suggestions: List[DomainSuggestion] = []
    processed = 0
    for _, row in companies_df.iterrows():
        if processed >= max_companies:
            break
        domain = str(row.get("domain") or "").strip()
        if not domain:
            continue
        bid = str(row.get("business_id") or "")
        name = str(row.get("name") or "")
        sug = suggest_for_company(bid, name, domain)
        if sug:
            suggestions.append(sug)
        processed += 1
    return pd.DataFrame([s.to_dict() for s in suggestions])


def _status_for_url(url: str) -> Dict[str, str]:
    try:
        resp = requests.get(
            url if url.startswith("http") else f"https://{url}",
            timeout=8,
            allow_redirects=True,
            headers={"User-Agent": "apprscan-domain-validate/0.1"},
        )
    except requests.RequestException as exc:
        return {"status": "fetch_failed", "reason": str(exc), "redirected_to": ""}
    final_url = str(resp.url)
    if resp.status_code >= 400:
        return {"status": f"http_{resp.status_code}", "reason": "", "redirected_to": ""}
    # consent hint
    text = resp.text.lower()
    if any(k in text for k in ["cookie", "consent", "eväste", "evaste", "hyväksy"]):
        return {"status": "consent_gate", "reason": "", "redirected_to": final_url if final_url != url else ""}
    redirected_to = final_url if final_url.rstrip("/") != url.rstrip("/") else ""
    return {"status": "ok", "reason": "", "redirected_to": redirected_to}


def validate_domains(domains_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in domains_df.iterrows():
        bid = str(r.get("business_id") or "").strip()
        name = str(r.get("name") or "").strip()
        domain = _clean_domain(str(r.get("domain") or ""))
        if not domain:
            rows.append({"business_id": bid, "name": name, "domain": "", "status": "no_domain", "redirected_to": "", "reason": ""})
            continue
        res = _status_for_url(domain)
        rows.append(
            {
                "business_id": bid,
                "name": name,
                "domain": domain,
                "status": res["status"],
                "redirected_to": res.get("redirected_to", ""),
                "reason": res.get("reason", ""),
            }
        )
    return pd.DataFrame(rows)
