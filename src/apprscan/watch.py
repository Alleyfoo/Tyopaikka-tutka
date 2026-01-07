"""Watch-mode report generation for new jobs."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def _shortlist_lookup(shortlist: Optional[pd.DataFrame]) -> Dict[str, Dict[str, object]]:
    if shortlist is None or shortlist.empty:
        return {}
    lookup: Dict[str, Dict[str, object]] = {}
    for _, row in shortlist.iterrows():
        bid = str(row.get("business_id") or row.get("businessId") or "").strip()
        if not bid:
            continue
        lookup[bid] = {
            "score": row.get("score"),
            "distance_km": row.get("distance_km"),
            "nearest_station": row.get("nearest_station"),
            "name": row.get("name") or row.get("company_name"),
        }
    return lookup


def generate_watch_report(shortlist: Optional[pd.DataFrame], jobs_diff: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    lines.append(f"Watch report generated: {now}")

    new_jobs = jobs_diff
    lines.append(f"New jobs: {len(new_jobs)}")
    lines.append("")

    if new_jobs.empty:
        lines.append("No new jobs found.")
        out_path.write_text("\n".join(lines), encoding="utf-8")
        return

    lookup = _shortlist_lookup(shortlist)

    # List new jobs
    lines.append("New job postings:")
    for _, row in new_jobs.iterrows():
        bid = str(row.get("company_business_id") or row.get("business_id") or "").strip()
        title = row.get("job_title") or ""
        url = row.get("job_url") or ""
        tags = row.get("tags") if isinstance(row.get("tags"), list) else []
        loc = row.get("location_text") or ""
        company_name = row.get("company_name") or lookup.get(bid, {}).get("name") or ""
        lines.append(f"- {company_name} ({bid}): {title}")
        if tags:
            lines.append(f"  tags: {', '.join(tags)}")
        if loc:
            lines.append(f"  location: {loc}")
        info = lookup.get(bid, {})
        extras = []
        if info.get("score") is not None:
            extras.append(f"score={info['score']}")
        if info.get("distance_km") is not None:
            extras.append(f"distance_km={info['distance_km']}")
        if info.get("nearest_station"):
            extras.append(f"station={info['nearest_station']}")
        if extras:
            lines.append("  " + ", ".join(extras))
        lines.append(f"  link: {url}")
        lines.append("")

    # Top companies by new job count
    lines.append("Top companies by new jobs:")
    bid_series = new_jobs["company_business_id"]
    if "business_id" in new_jobs.columns:
        bid_series = bid_series.fillna(new_jobs["business_id"])
    counts = (
        new_jobs.assign(bid=bid_series)
        .groupby("bid")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    for _, r in counts.iterrows():
        bid = str(r["bid"])
        name = lookup.get(bid, {}).get("name") or ""
        lines.append(f"- {name} ({bid}): {int(r['count'])}")

    out_path.write_text("\n".join(lines), encoding="utf-8")
