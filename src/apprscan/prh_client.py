"""PRH/YTJ API client with pagination and retry."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import requests

PRH_BASE = "https://avoindata.prh.fi/opendata-ytj-api/v3"
DEFAULT_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF = 1.0


def _should_retry(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code < 600


def _request_with_retry(
    session: requests.Session,
    url: str,
    params: Dict[str, Any],
    timeout: float,
    max_retries: int,
    backoff_factor: float,
) -> requests.Response:
    for attempt in range(max_retries):
        resp = session.get(url, params=params, timeout=timeout)
        if _should_retry(resp.status_code) and attempt < max_retries - 1:
            sleep_for = backoff_factor * (2**attempt)
            time.sleep(sleep_for)
            continue
        resp.raise_for_status()
        return resp
    # Should never reach here due to raise_for_status.
    return resp


def fetch_companies(
    location: str,
    main_business_line: Optional[str] = None,
    reg_start: Optional[str] = None,
    reg_end: Optional[str] = None,
    max_pages: int = 0,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF,
) -> List[Dict[str, Any]]:
    """Fetch companies for a single location with pagination."""
    sess = session or requests.Session()
    page = 0
    companies: List[Dict[str, Any]] = []
    url = f"{PRH_BASE}/companies"

    while True:
        params: Dict[str, Any] = {"location": location, "page": page}
        if main_business_line:
            params["mainBusinessLine"] = main_business_line
        if reg_start:
            params["registrationDateStart"] = reg_start
        if reg_end:
            params["registrationDateEnd"] = reg_end

        resp = _request_with_retry(
            sess, url, params=params, timeout=timeout, max_retries=max_retries, backoff_factor=backoff_factor
        )
        data = resp.json()
        rows = data.get("companies") or data.get("results") or []
        if not rows:
            break

        companies.extend(rows)
        page += 1

        if max_pages and page >= max_pages:
            break

        total = data.get("totalResults")
        if total is not None and page * 100 >= int(total):
            break

    return companies
