#!/usr/bin/env python
"""Find catalog-heavy webshops among the retail/wholesale companies.

The lead scoring (`apprscan.leads`) ranks companies by their TOI code, not by
what their website actually is — so a small boutique and a Bygghemma-style
catalog store score the same. This script probes each unique live retail
(vähittäiskauppa, TOI 47) / wholesale (tukkukauppa, TOI 46) website and answers
the question the user actually asks: **does this company run a webshop with a
large online catalog?**

Signals, per unique host:
- E-commerce platform fingerprint from the homepage HTML (Shopify, MyCashflow,
  WooCommerce, Magento, PrestaShop, ...).
- Cart/checkout text ("ostoskori", "Lisää ostoskoriin", add-to-cart).
- Catalog size estimate: product URLs in the sitemap (via robots.txt) when
  available, else product links on the homepage. Product URL patterns cover
  the Finnish permalinks (/tuote/) plus the common platforms (/products/,
  /catalog/product/view, ...).

Everything is cached in ``data/webshop_cache.sqlite`` keyed by host, so
re-runs are free and offline. Output is a company-level CSV
``out/webshops.csv`` ranked by shop score, with the catalog estimate and the
detected platform — a shortlist of Bygghemma / Puolenkuun Pelit style
companies, not just "has a website".

Usage
-----
  python scripts/find_webshops.py                  # probe all live 46/47 sites
  python scripts/find_webshops.py --groups 47      # retail only
  python scripts/find_webshops.py --top 40         # print the top 40
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

_ROOT = Path(__file__).resolve().parents[1]
CSV_IN = _ROOT / "out" / "companies.csv"
CSV_OUT = _ROOT / "out" / "webshops.csv"
CACHE_PATH = _ROOT / "data" / "webshop_cache.sqlite"

REQUEST_DELAY = 0.3
TIMEOUT = 12
HEADERS = {
    "User-Agent": "TyopaikkaSkanneri/1.0 (webshop detection)",
    "Accept-Language": "fi",
}
# Cap parsing so a runaway sitemap can't dominate a run.
MAX_SITEMAP_URLS = 20_000
MAX_SITEMAPS_PER_HOST = 5

# Platform fingerprints: substring -> platform name (lowercase HTML match).
PLATFORMS = {
    "cdn.shopify.com": "Shopify",
    "/cdn/shop/": "Shopify",
    "shopify": "Shopify",
    "mycashflow": "MyCashflow",
    "woocommerce": "WooCommerce",
    "wp-content/plugins/woocommerce": "WooCommerce",
    "magento": "Magento",
    "prestashop": "PrestaShop",
    "mage/": "Magento",
    "wixstatic": "Wix",
    "static.wixstatic": "Wix",
    "squarespace": "Squarespace",
    "netrauta": "Netrauta",
    "rajala": "Rajala",
}

# Cart/checkout signals, lowercase HTML match.
CART_SIGNALS = (
    "ostoskori", "lisää ostoskoriin", "add to cart", "add-to-cart",
    "add_to_cart", "koriin", "shoppailla", "checkout",
)

# Product URL patterns: a match means the URL is a product page, not a
# category / info page. Finnish permalinks first, then platform defaults.
PRODUCT_URL_RE = re.compile(
    r"/tuote/|/tuotteet/|/product/|/products/|/catalog/product/view|"
    r"/p/\d|/kauppa/.+|/catalog/product|/en/product|/fi/product|"
    r"/collections/.+/products/|/verkkokauppa/.+/.+|/\d{4,}[/-][a-z0-9-]+\.html"
)

SITEMAP_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.IGNORECASE)


def _ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS webshop_cache ("
        "host TEXT PRIMARY KEY, result TEXT, ts TEXT)"
    )
    conn.commit()


def _cache_get(conn, host: str) -> dict | None:
    row = conn.execute(
        "SELECT result FROM webshop_cache WHERE host = ?", (host,)
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None


def _cache_set(conn, host: str, result: dict) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO webshop_cache(host, result, ts) "
        "VALUES (?, ?, datetime('now'))",
        (host, json.dumps(result, ensure_ascii=False)),
    )
    conn.commit()


def _get(session: requests.Session, url: str) -> str | None:
    """GET a page, returning lowercase HTML or None."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        if resp.status_code != 200 or not resp.text:
            return None
        return resp.text.lower()
    except requests.RequestException:
        return None


def _detect_platform(html: str) -> str | None:
    for needle, name in PLATFORMS.items():
        if needle in html:
            return name
    return None


def _count_product_links(html: str) -> int:
    hrefs = re.findall(r'href="([^"]+)"', html)
    return len({h for h in hrefs if PRODUCT_URL_RE.search(h)})


def _has_cart(html: str) -> bool:
    return any(sig in html for sig in CART_SIGNALS)


def _sitemap_product_count(session: requests.Session, base_url: str) -> int:
    """Estimate catalog size from the sitemap chain (robots.txt first).

    Returns 0 when no sitemap exists or it lists no product URLs — the caller
    then falls back to homepage product links.
    """
    robots = _get(session, urljoin(base_url, "/robots.txt"))
    sitemap_urls: list[str] = []
    if robots:
        for line in robots.splitlines():
            if line.strip().startswith("sitemap:"):
                sitemap_urls.append(line.split(":", 1)[1].strip())
    if not sitemap_urls:
        sitemap_urls.append(urljoin(base_url, "/sitemap.xml"))
    sitemap_urls = sitemap_urls[:MAX_SITEMAPS_PER_HOST]

    product_urls: set[str] = set()
    total_locs = 0
    for sm_url in sitemap_urls:
        xml = _get(session, sm_url)
        if xml is None:
            continue
        locs = SITEMAP_LOC_RE.findall(xml)
        # Sitemap index: recurse one level into child sitemaps (product URLs
        # usually live in e.g. sitemap_products_1.xml).
        for loc in locs:
            if total_locs >= MAX_SITEMAP_URLS:
                break
            if loc.endswith(".xml") and "sitemap" in loc:
                child = _get(session, loc)
                if child is None:
                    continue
                child_locs = SITEMAP_LOC_RE.findall(child)
                for c in child_locs:
                    total_locs += 1
                    if PRODUCT_URL_RE.search(c):
                        product_urls.add(c.split("?")[0])
                    if total_locs >= MAX_SITEMAP_URLS:
                        break
            else:
                total_locs += 1
                if PRODUCT_URL_RE.search(loc):
                    product_urls.add(loc.split("?")[0])
        if total_locs >= MAX_SITEMAP_URLS:
            break
    return len(product_urls)


def _probe_host(session: requests.Session, url: str) -> dict:
    """Probe one website; result is cached per host."""
    html = _get(session, url)
    if html is None:
        # Retry with www stripped/added — PRH registry URLs are not always
        # the live host.
        parsed = urlparse(url)
        alt = (parsed._replace(netloc=parsed.netloc.removeprefix("www.")).geturl()
               if "www." in parsed.netloc else
               f"{parsed.scheme}://www.{parsed.netloc}")
        html = _get(session, alt)
    if html is None:
        return {"reachable": False}

    result = {
        "reachable": True,
        "platform": _detect_platform(html),
        "cart_signal": _has_cart(html),
        "homepage_product_links": _count_product_links(html),
    }
    time.sleep(REQUEST_DELAY)
    result["sitemap_products"] = _sitemap_product_count(session, url)
    return result


def _shop_score(r: dict) -> int:
    """Rank a probe result: catalog size and e-commerce platform dominate."""
    if not r.get("reachable"):
        return 0
    score = 0
    if r.get("platform"):
        score += 3
    if r.get("cart_signal"):
        score += 2
    sitemap_n = r.get("sitemap_products") or 0
    home_n = r.get("homepage_product_links") or 0
    catalog = sitemap_n if sitemap_n else home_n
    if sitemap_n >= 500 or home_n >= 30:
        score += 4
    elif sitemap_n >= 100 or home_n >= 10:
        score += 3
    elif sitemap_n >= 20 or home_n >= 3:
        score += 2
    elif catalog > 0:
        score += 1
    return score


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default=str(CSV_IN), help="companies.csv path")
    ap.add_argument("--out", default=str(CSV_OUT), help="output CSV path")
    ap.add_argument(
        "--groups", default="46,47",
        help="TOI groups to include (comma-separated 2-char prefixes)",
    )
    ap.add_argument("--status", default="live", help="Required website_status, or 'any'")
    ap.add_argument("--refresh", action="store_true", help="Ignore the host cache")
    ap.add_argument("--top", type=int, default=30, help="Print this many top shops")
    args = ap.parse_args()

    groups = tuple(g.strip() for g in args.groups.split(",") if g.strip())

    df = pd.read_csv(args.csv, encoding="utf-8-sig", dtype=str)
    df["best_website"] = df["website"].combine_first(df.get("found_website"))
    sel = df[df["toi_code"].str.startswith(groups, na=False)].copy()
    sel = sel[sel["best_website"].notna()]
    if args.status != "any" and "website_status" in sel.columns:
        sel = sel[sel["website_status"] == args.status]

    sel["_host"] = (
        sel["best_website"].str.extract(r"https?://([^/]+)", expand=False).str.lower()
    )
    hosts = sorted(sel["_host"].dropna().unique())
    print(f"{len(sel)} live {groups} companies on {len(hosts)} unique hosts")

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(CACHE_PATH)
    _ensure_db(conn)
    session = requests.Session()

    results: dict[str, dict] = {}
    n_fetch = n_cache = 0
    for i, host in enumerate(hosts, 1):
        cached = None if args.refresh else _cache_get(conn, host)
        if cached is not None:
            results[host] = cached
            n_cache += 1
            continue
        url = f"https://{host}" if not host.startswith("http") else host
        res = _probe_host(session, url)
        results[host] = res
        _cache_set(conn, host, res)
        n_fetch += 1
        if n_fetch % 20 == 0:
            print(f"  probed {i}/{len(hosts)} (fetched {n_fetch}, cached {n_cache}) ...")
        time.sleep(REQUEST_DELAY)
    conn.close()
    print(f"Probed {len(hosts)} hosts (fetched {n_fetch}, cached {n_cache})")

    # Company-level table: attach the host probe to every company.
    def _field(r, key):
        return (r or {}).get(key) if isinstance(r, dict) else None

    sel["_probe"] = sel["_host"].map(results)
    sel["shop_platform"] = sel["_probe"].map(lambda r: _field(r, "platform"))
    sel["shop_cart_signal"] = sel["_probe"].map(lambda r: _field(r, "cart_signal"))
    sel["shop_sitemap_products"] = sel["_probe"].map(
        lambda r: _field(r, "sitemap_products"))
    sel["shop_homepage_product_links"] = sel["_probe"].map(
        lambda r: _field(r, "homepage_product_links"))
    sel["shop_score"] = sel["_probe"].map(
        lambda r: _shop_score(r) if isinstance(r, dict) else 0)
    sel["shop_catalog_estimate"] = sel.apply(
        lambda row: (row["shop_sitemap_products"]
                    if pd.notna(row["shop_sitemap_products"])
                    and float(row["shop_sitemap_products"] or 0) > 0
                    else row["shop_homepage_product_links"]),
        axis=1,
    )

    out = sel.drop(columns=["_probe"]).sort_values(
        ["shop_score", "shop_catalog_estimate"], ascending=[False, False]
    )
    out.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"Wrote {args.out} ({len(out)} companies)")

    shops = out[out["shop_score"] >= 5].drop_duplicates("_host")
    print(f"\nTop webshop candidates (score >= 5): {len(shops)} companies")
    show = shops.head(args.top)
    for _, r in show.iterrows():
        cat = r["shop_catalog_estimate"]
        cat_txt = f"{int(float(cat)):>5} tuotetta" if pd.notna(cat) and str(cat) != "nan" else "  ?"
        print(
            f"  {r['shop_score']:>2}  {cat_txt}  {str(r['shop_platform'] or '-'):12s} "
            f"{r['name'][:38]:38s} {r['_host']}"
        )


if __name__ == "__main__":
    main()