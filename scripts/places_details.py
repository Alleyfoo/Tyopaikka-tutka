#!/usr/bin/env python
"""Fetch missing website URLs for Places entries using place_id."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd

from apprscan.places_api import fetch_place_details, get_api_key


def _missing(val: object) -> bool:
    if val is None:
        return True
    text = str(val).strip()
    return not text or text.lower() in {"nan", "none", "null"}


def _clean_domain(url: str) -> str:
    url = (url or "").strip()
    if not url or url.lower() in {"nan", "none", "null"}:
        return ""
    if "://" not in url:
        url = f"https://{url}"
    try:
        host = url.split("://", 1)[1].split("/", 1)[0]
    except IndexError:
        return ""
    return host.strip()


def _load_master(path: Path, sheet: str) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        try:
            return pd.read_excel(path, sheet_name=sheet)
        except ValueError:
            return pd.read_excel(path, sheet_name=0)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError("Unsupported master format (use xlsx/csv/parquet).")


def _require_api_key() -> None:
    try:
        get_api_key()
    except RuntimeError:
        print("GOOGLE_MAPS_API_KEY is not set.", file=sys.stderr)
        print("Set it in PowerShell: $env:GOOGLE_MAPS_API_KEY='YOUR_KEY'", file=sys.stderr)
        sys.exit(2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Places website URLs by place_id.")
    parser.add_argument("--master", default="out/master_places.xlsx", help="Input master (xlsx/csv/parquet).")
    parser.add_argument("--sheet", default="Shortlist", help="Sheet name when using xlsx.")
    parser.add_argument("--out", default="out/places_websites.csv", help="Output CSV path.")
    parser.add_argument("--sleep-s", type=float, default=0.2, help="Sleep between requests.")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to fetch (0=all).")
    parser.add_argument("--all-rows", action="store_true", help="Fetch all rows (not just missing website.url).")
    parser.add_argument("--update-domains", default="", help="Optional domains.csv to update.")
    parser.add_argument("--domains-out", default="", help="Output path for updated domains CSV.")
    args = parser.parse_args()

    _require_api_key()
    master_path = Path(args.master)
    df = _load_master(master_path, args.sheet)
    if "business_id" not in df.columns and "place_id" in df.columns:
        df = df.rename(columns={"place_id": "business_id"})
    if "website.url" not in df.columns:
        df["website.url"] = ""

    if args.all_rows:
        target = df
    else:
        target = df[df["website.url"].apply(_missing)]

    if args.limit:
        target = target.head(args.limit)

    rows: list[dict[str, str]] = []
    for _, row in target.iterrows():
        place_id = str(row.get("business_id") or "").strip()
        if not place_id:
            continue
        name = str(row.get("name") or "")
        try:
            details = fetch_place_details(place_id)
            website = str(details.get("website") or "")
            rows.append(
                {
                    "business_id": place_id,
                    "name": details.get("name") or name,
                    "formatted_address": details.get("formatted_address") or "",
                    "website.url": website,
                    "status": "ok" if website else "no_website",
                    "reason": "",
                }
            )
        except RuntimeError as exc:
            rows.append(
                {
                    "business_id": place_id,
                    "name": name,
                    "formatted_address": "",
                    "website.url": "",
                    "status": "error",
                    "reason": str(exc),
                }
            )
        if args.sleep_s:
            time.sleep(args.sleep_s)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows)
    out_df.to_csv(out_path, index=False)
    print(f"Wrote websites: {out_path} ({len(out_df)} rows)")

    if args.update_domains:
        domains_path = Path(args.update_domains)
        dom_df = pd.read_csv(domains_path)
        if "business_id" not in dom_df.columns and "businessId" in dom_df.columns:
            dom_df = dom_df.rename(columns={"businessId": "business_id"})
        if "domain" not in dom_df.columns:
            dom_df["domain"] = ""
        domain_map = {
            str(r["business_id"]): _clean_domain(str(r.get("website.url") or ""))
            for _, r in out_df.iterrows()
        }
        def _fill_domain(row: pd.Series) -> str:
            current = str(row.get("domain") or "").strip()
            if current and current.lower() not in {"nan", "none", "null"}:
                return current
            bid = str(row.get("business_id") or "")
            return domain_map.get(bid, "")

        dom_df["domain"] = dom_df.apply(_fill_domain, axis=1)
        domains_out = Path(args.domains_out) if args.domains_out else domains_path.with_name(
            f"{domains_path.stem}_updated.csv"
        )
        dom_df.to_csv(domains_out, index=False)
        print(f"Updated domains: {domains_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
