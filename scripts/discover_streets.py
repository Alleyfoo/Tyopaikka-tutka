#!/usr/bin/env python
"""Discover street names within a radius of each target station using OpenStreetMap Overpass API.

Outputs a YAML-friendly summary of unique street names per area, ready
to be copied into the area config or used directly as PRH location queries.

Usage:
    python scripts/discover_streets.py
    python scripts/discover_streets.py --radius-m 1500 --out data/area_streets.json
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any

import requests

OVERPASS_MIRRORS = [
    "https://overpass.kumi.systems/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass-api.de/api/interpreter",
]

# Target station areas: (label, lat, lon, municipality_for_PRH)
STATIONS = [
    {"label": "Lahti", "lat": 60.9764, "lon": 25.6574, "municipality": "Lahti"},
    {"label": "Kerava", "lat": 60.4043, "lon": 25.1057, "municipality": "Kerava"},
    {"label": "Savio", "lat": 60.3804, "lon": 25.0977, "municipality": "Kerava"},
    {"label": "Pasila", "lat": 60.1987, "lon": 24.9334, "municipality": "Helsinki"},
    {"label": "Tikkurila", "lat": 60.2925, "lon": 25.0439, "municipality": "Vantaa"},
    {"label": "Järvenpää", "lat": 60.4733, "lon": 25.0903, "municipality": "Järvenpää"},
    {"label": "Hyvinkää", "lat": 60.6310, "lon": 24.8590, "municipality": "Hyvinkää"},
    {"label": "Riihimäki", "lat": 60.7186, "lon": 24.7740, "municipality": "Riihimäki"},
    # Oikorata stations between Kerava and Mäntsälä/Lahti — small, mostly
    # residential areas around otherwise rural stations.
    {"label": "Haarajoki", "lat": 60.5363, "lon": 25.1198, "municipality": "Järvenpää"},
    {"label": "Henna", "lat": 60.7284, "lon": 25.5270, "municipality": "Mäntsälä"},
]


def _overpass_streets(lat: float, lon: float, radius_m: int) -> list[str]:
    """Return sorted unique street names within radius of (lat, lon)."""
    query = (
        f"[out:json][timeout:60];"
        f'way["highway"]["name"](around:{radius_m},{lat},{lon});'
        f"out tags;"
    )
    last_exc: Exception | None = None
    for mirror in OVERPASS_MIRRORS:
        try:
            resp = requests.get(
                mirror,
                params={"data": query},
                timeout=70,
                # The default python-requests User-Agent is rejected with 406.
                headers={"User-Agent": "TyopaikkaSkanneri/1.0 (street discovery)"},
            )
            resp.raise_for_status()
            data = resp.json()
            names: set[str] = set()
            for element in data.get("elements", []):
                name = element.get("tags", {}).get("name")
                if name:
                    names.add(name)
            return sorted(names)
        except Exception as exc:
            print(f"    [mirror {mirror} failed: {exc}]")
            last_exc = exc
            time.sleep(3)
    raise RuntimeError(f"All mirrors failed. Last error: {last_exc}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover streets near target stations via Overpass API."
    )
    parser.add_argument(
        "--radius-m",
        type=int,
        default=1500,
        help="Search radius in metres (default: 1500)",
    )
    parser.add_argument("--out", default=None, help="Optional JSON output file path")
    parser.add_argument(
        "--only",
        nargs="*",
        default=None,
        metavar="LABEL",
        help=(
            "Only discover these area labels. With --out pointing at an existing "
            "file, the other areas are kept as-is instead of being overwritten "
            "(no Overpass queries are made for them)."
        ),
    )
    args = parser.parse_args()

    # With --only, keep the untouched areas from an existing output file so a
    # partial refresh doesn't blank the rest of the config.
    existing: list[dict[str, Any]] = []
    if args.only is not None and args.out:
        import pathlib

        out_path = pathlib.Path(args.out)
        if out_path.exists():
            existing = [
                r
                for r in json.loads(out_path.read_text(encoding="utf-8"))
                if r["label"] not in set(args.only)
            ]
            print(f"Keeping {len(existing)} existing areas from {args.out}")

    targets = [st for st in STATIONS if args.only is None or st["label"] in set(args.only)]
    if not targets:
        raise SystemExit(f"No station matches --only {args.only}")

    results: list[dict[str, Any]] = []

    for i, st in enumerate(targets):
        if i > 0:
            time.sleep(2)  # be polite to Overpass servers

        print(f"\n{'='*60}")
        print(f"  {st['label']}  ({st['lat']}, {st['lon']})  radius={args.radius_m}m")
        print(f"  PRH municipality: {st['municipality']}")
        print("=" * 60)

        try:
            streets = _overpass_streets(st["lat"], st["lon"], args.radius_m)
        except Exception as exc:
            print(f"  ERROR: {exc}")
            streets = []

        print(f"  {len(streets)} unique street names found:")
        for s in streets:
            print(f"    - {s}")

        results.append(
            {
                "label": st["label"],
                "lat": st["lat"],
                "lon": st["lon"],
                "municipality": st["municipality"],
                "radius_m": args.radius_m,
                "streets": streets,
            }
        )

    if args.out:
        import pathlib

        pathlib.Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        merged = existing + results
        pathlib.Path(args.out).write_text(
            json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nSaved to {args.out}")
    else:
        print("\n\n--- JSON summary ---")
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
