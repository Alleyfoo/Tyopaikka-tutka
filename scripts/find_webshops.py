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
  python scripts/find_webshops.py    # probe 46/47 sites (live + WAF-blocked)
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
TIMEOUT_SITEMAP = 30  # big sitemap children are ~5-10 MB; the homepage timeout chokes on those
HEADERS = {
    "User-Agent": "TyopaikkaSkanneri/1.0 (webshop detection)",
    "Accept-Language": "fi",
}
# Cap parsing so a runaway sitemap can't dominate a run. 100k keeps the true
# size visible for giants (taloon.com ships ~200k URLs in 4 children); a host
# that hits the cap is marked capped so its estimate reads as a floor.
MAX_SITEMAP_URLS = 100_000
MAX_SITEMAPS_PER_HOST = 10

# Bump when probe logic changes (patterns, caps, recursion) — cache entries
# written by an older version are ignored and re-probed.
PROBE_VERSION = 2

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
# category / info page. Finnish permalinks first, then platform defaults
# (incl. the osC/Zen Cart product_info.php used by e.g. puolenkuunpelit.com).
PRODUCT_URL_RE = re.compile(
    r"/tuote/|/tuotteet/|/product/|/products/|/catalog/product/view|"
    r"/p/\d|/kauppa/.+|/catalog/product|/en/product|/fi/product|"
    r"product_info\.php|/collections/.+/products/|/verkkokauppa/.+/.+|"
    r"/\d{4,}[/-][a-z0-9-]+\.html"
)

SITEMAP_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.IGNORECASE)

# Product pages whose identity lives in the QUERY string (osC/Zen Cart style
# product_info.php?products_id=N, Magento's catalog/product/view?id=N): the
# query must be kept or every product collapses into one URL.
QUERY_KEYED_RE = re.compile(r"product_info\.php|catalog/product/view|/index\.php\?")


def _product_key(url: str) -> str:
    """Dedupe key for a product URL, dropping query only when it is tracking noise."""
    path = url.split("?")[0]
    if QUERY_KEYED_RE.search(url):
        return url
    return path


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
        result = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return None
    # Entries from an older probe version are stale by definition.
    if not isinstance(result, dict) or result.get("_v") != PROBE_VERSION:
        return None
    return result


def _cache_set(conn, host: str, result: dict) -> None:
    stored = dict(result)
    stored["_v"] = PROBE_VERSION
    conn.execute(
        "INSERT OR REPLACE INTO webshop_cache(host, result, ts) "
        "VALUES (?, ?, datetime('now'))",
        (host, json.dumps(stored, ensure_ascii=False)),
    )
    conn.commit()


def _get(session: requests.Session, url: str, timeout: float = TIMEOUT) -> str | None:
    """GET a page, returning lowercase HTML or None."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
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


def _sitemap_product_count(session: requests.Session, base_url: str) -> dict:
    """Estimate catalog size from the sitemap chain (robots.txt first).

    Returns {"products": <product URLs>, "urls": <all URLs seen>, "capped":
    whether the MAX_SITEMAP_URLS cap was hit}. Child sitemaps are recursed one
    level under ANY name — shops split theirs as products_part_1.xml (osC)
    just as often as sitemap_products_1.xml (Shopify).
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
        xml = _get(session, sm_url, timeout=TIMEOUT_SITEMAP)
        if xml is None:
            continue
        locs = SITEMAP_LOC_RE.findall(xml)
        # Sitemap index: recurse one level into child sitemaps (product URLs
        # usually live in e.g. sitemap_products_1.xml or products_part_1.xml).
        for loc in locs:
            if total_locs >= MAX_SITEMAP_URLS:
                break
            if loc.endswith(".xml"):
                child = _get(session, loc, timeout=TIMEOUT_SITEMAP)
                if child is None:
                    continue
                child_locs = SITEMAP_LOC_RE.findall(child)
                for c in child_locs:
                    total_locs += 1
                    if PRODUCT_URL_RE.search(c):
                        product_urls.add(_product_key(c))
                    if total_locs >= MAX_SITEMAP_URLS:
                        break
            else:
                total_locs += 1
                if PRODUCT_URL_RE.search(loc):
                    product_urls.add(_product_key(loc))
        if total_locs >= MAX_SITEMAP_URLS:
            break
    return {
        "products": len(product_urls),
        "urls": total_locs,
        "capped": total_locs >= MAX_SITEMAP_URLS,
    }


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
        # Homepage blocked (WAF/bot challenge), but robots.txt and sitemaps
        # are usually still served — measure the catalog from the chain alone.
        sitemap = _sitemap_product_count(session, url)
        return {
            "reachable": False,
            "platform": None,
            "cart_signal": None,
            "homepage_product_links": 0,
            "sitemap_products": sitemap["products"],
            "sitemap_urls": sitemap["urls"],
            "sitemap_capped": sitemap["capped"],
        }

    result = {
        "reachable": True,
        "platform": _detect_platform(html),
        "cart_signal": _has_cart(html),
        "homepage_product_links": _count_product_links(html),
    }
    time.sleep(REQUEST_DELAY)
    sitemap = _sitemap_product_count(session, url)
    result["sitemap_products"] = sitemap["products"]
    result["sitemap_urls"] = sitemap["urls"]
    result["sitemap_capped"] = sitemap["capped"]
    return result


def _shop_score(r: dict) -> int:
    """Rank a probe result: catalog size and e-commerce platform dominate."""
    if not r.get("reachable") and not (r.get("sitemap_products") or r.get("sitemap_urls")):
        return 0
    score = 0
    if r.get("reachable"):
        if r.get("platform"):
            score += 3
        if r.get("cart_signal"):
            score += 2
    elif (r.get("sitemap_products") or 0) >= 20:
        # Homepage blocked, but the sitemap carries product-shaped URLs
        # (product_info.php?products_id=N ...): that is e-commerce evidence
        # in itself — a WAF-blocked giant shouldn't rank below a boutique.
        score += 2
    sitemap_n = r.get("sitemap_products") or 0
    home_n = r.get("homepage_product_links") or 0
    # Flat-slug catalogs: product patterns match nothing, but a huge sitemap
    # on an e-commerce site still measures the catalog (e.g. taloon.com).
    flat_n = 0
    if not sitemap_n and not home_n:
        su = r.get("sitemap_urls") or 0
        if su >= 500 and (r.get("platform") or r.get("cart_signal")):
            flat_n = su
    if sitemap_n >= 500 or home_n >= 30 or flat_n >= 500:
        score += 4
    elif sitemap_n >= 100 or home_n >= 10 or flat_n >= 100:
        score += 3
    elif sitemap_n >= 20 or home_n >= 3 or flat_n >= 20:
        score += 2
    elif (sitemap_n or home_n or flat_n) > 0:
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
    ap.add_argument(
        "--status", default="live,unreachable",
        help="Comma-separated allowed website_status values, or 'any'. "
             "'unreachable' is included by default because a WAF/bot challenge "
             "(403) hides real shops — e.g. puolenkuunpelit.com — from the "
             "liveness probe; the sitemap chain still measures them.",
    )
    ap.add_argument("--refresh", action="store_true", help="Ignore the host cache")
    ap.add_argument("--top", type=int, default=30, help="Print this many top shops")
    args = ap.parse_args()

    groups = tuple(g.strip() for g in args.groups.split(",") if g.strip())

    df = pd.read_csv(args.csv, encoding="utf-8-sig", dtype=str)
    df["best_website"] = df["website"].combine_first(df.get("found_website"))
    # PRH registry websites often come scheme-less ("www.taloon.com") —
    # normalize before extracting the host or they are silently skipped
    # (79% of the first run's companies were dropped this way).
    df["best_website"] = df["best_website"].map(
        lambda u: u
        if not isinstance(u, str) or not u.strip()
        else (u.strip() if u.strip().lower().startswith(("http://", "https://"))
              else "https://" + u.strip())
    )
    sel = df[df["toi_code"].str.startswith(groups, na=False)].copy()
    sel = sel[sel["best_website"].notna()]
    if args.status != "any" and "website_status" in sel.columns:
        allowed = {v.strip() for v in args.status.split(",") if v.strip()}
        sel = sel[sel["website_status"].isin(allowed)]

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
    sel["shop_sitemap_urls"] = sel["_probe"].map(
        lambda r: _field(r, "sitemap_urls"))
    sel["shop_sitemap_capped"] = sel["_probe"].map(
        lambda r: _field(r, "sitemap_capped"))
    sel["shop_score"] = sel["_probe"].map(
        lambda r: _shop_score(r) if isinstance(r, dict) else 0)

    def _catalog_estimate(row):
        """Best catalog estimate: product URLs first, big-flat-sitemap floor second."""
        sp = row["shop_sitemap_products"]
        hl = row["shop_homepage_product_links"]
        if pd.notna(sp) and float(sp or 0) > 0:
            return sp
        if pd.notna(hl) and float(hl or 0) > 0:
            return hl
        # Product patterns matched nothing but the sitemap is huge on an
        # e-commerce site (flat slugs, e.g. taloon.com): the raw sitemap
        # size is a floor for the catalog.
        su = row["shop_sitemap_urls"]
        ecommerce = pd.notna(row["shop_platform"]) or bool(row["shop_cart_signal"])
        if ecommerce and pd.notna(su) and float(su or 0) >= 500:
            return su
        return hl  # may be NaN

    sel["shop_catalog_estimate"] = sel.apply(_catalog_estimate, axis=1)

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
        floor = "+" if str(r.get("shop_sitemap_capped")) in ("True", "true") else ""
        cat_txt = f"{int(float(cat)):>5}{floor} tuotetta" if pd.notna(cat) and str(cat) != "nan" else "  ?"
        print(
            f"  {r['shop_score']:>2}  {cat_txt}  {str(r['shop_platform'] or '-'):12s} "
            f"{r['name'][:38]:38s} {r['_host']}"
        )


if __name__ == "__main__":
    main()