"""Command-line interface for apprscan."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

import pandas as pd

from . import __version__
from .geocode import geocode_address
from .normalize import normalize_companies
from .prh_client import fetch_companies
from .report import export_reports
from .stations import load_stations
from .distance import nearest_station_from_df


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apprscan",
        description="Apprenticeship employer scanner (PRH/YTJ + geokoodaus + raportointi).",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command", required=False)

    run_parser = subparsers.add_parser(
        "run",
        help="Suorita haku ja raportointi.",
        description=(
            "Hakee PRH/YTJ:stä yrityksiä, geokoodaa osoitteet ja tuottaa raportit "
            "(Excel/GeoJSON/HTML)."
        ),
    )
    run_parser.add_argument(
        "--cities",
        type=str,
        help="Pilkuilla erotettu kaupunkilista (esim. Helsinki,Espoo,Vantaa,Lahti).",
    )
    run_parser.add_argument(
        "--radius-km",
        type=float,
        default=1.0,
        help="Suurin etäisyys (km) lähimmälle asemalle.",
    )
    run_parser.add_argument(
        "--main-business-line",
        type=str,
        default="",
        help="PRH mainBusinessLine -suodatin.",
    )
    run_parser.add_argument("--reg-start", type=str, default="", help="registrationDateStart (YYYY-MM-DD).")
    run_parser.add_argument("--reg-end", type=str, default="", help="registrationDateEnd (YYYY-MM-DD).")
    run_parser.add_argument("--max-pages", type=int, default=0, help="Maksimi sivut per kaupunki (0 = kaikki).")
    run_parser.add_argument(
        "--stations-file",
        type=str,
        default=None,
        help="Paikallinen asemadata CSV (station_name,lat,lon). Oletus: data/stations_fi.csv jos löytyy.",
    )
    run_parser.add_argument("--skip-geocode", action="store_true", help="Ohita geokoodaus (debug / nopea ajo).")
    run_parser.add_argument("--limit", type=int, default=0, help="Käsittele vain N ensimmäistä riviä (debug).")
    run_parser.add_argument(
        "--out",
        type=str,
        default="out",
        help="Tulosten hakemisto (Excel/GeoJSON/HTML).",
    )
    run_parser.set_defaults(func=run_command)

    return parser


def run_command(args: argparse.Namespace) -> int:
    cities = [c.strip() for c in (args.cities or "").split(",") if c.strip()]
    if not cities:
        print("Anna vähintään yksi kaupunki --cities parametrilla.")
        return 1

    all_rows = []
    for city in cities:
        rows = fetch_companies(
            location=city,
            main_business_line=args.main_business_line or None,
            reg_start=args.reg_start or None,
            reg_end=args.reg_end or None,
            max_pages=args.max_pages,
        )
        for r in rows:
            r["_source_city"] = city
        all_rows.extend(rows)

    if args.limit:
        all_rows = all_rows[: args.limit]

    print(f"Haettu rivejä: {len(all_rows)}")
    df = normalize_companies(all_rows)
    if df.empty:
        print("Ei rivejä käsiteltäväksi.")
        return 0

    built_addresses = int(df["full_address"].astype(bool).sum())
    print(f"Muodostetut osoitteet: {built_addresses} / {len(df)}")

    if args.skip_geocode:
        df["lat"] = None
        df["lon"] = None
        df["geocode_provider"] = None
        df["geocode_cache_hit"] = None
    else:
        lats = []
        lons = []
        providers = []
        cache_hits = []
        for addr in df["full_address"]:
            lat, lon, provider, cached = geocode_address(addr)
            lats.append(lat)
            lons.append(lon)
            providers.append(provider)
            cache_hits.append(cached)
        df["lat"] = lats
        df["lon"] = lons
        df["geocode_provider"] = providers
        df["geocode_cache_hit"] = cache_hits

    stations_df = None
    try:
        stations_df = load_stations(use_local=True, path=args.stations_file)
    except Exception as exc:  # pragma: no cover - defensive logging
        print(f"Asemadatan lataus epäonnistui: {exc}")

    nearest_names = []
    nearest_dists = []
    if stations_df is not None and not df[["lat", "lon"]].isna().all().all():
        for _, row in df.iterrows():
            if pd.isna(row.get("lat")) or pd.isna(row.get("lon")):
                nearest_names.append("")
                nearest_dists.append(None)
                continue
            name, dist = nearest_station_from_df(float(row["lat"]), float(row["lon"]), stations_df)
            nearest_names.append(name)
            nearest_dists.append(dist)
    else:
        nearest_names = [""] * len(df)
        nearest_dists = [None] * len(df)

    df["nearest_station"] = nearest_names
    df["distance_km"] = nearest_dists

    export_reports(df, args.out)
    print(f"Raportit kirjoitettu hakemistoon: {args.out}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 0
    return args.func(args)
