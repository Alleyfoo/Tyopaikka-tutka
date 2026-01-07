"""Reporting utilities (Excel, GeoJSON, HTML map)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

import folium
import pandas as pd


def write_excel(shortlist: pd.DataFrame, path: str, excluded: Optional[pd.DataFrame] = None) -> None:
    with pd.ExcelWriter(path) as writer:
        shortlist.to_excel(writer, index=False, sheet_name="Shortlist")
        if excluded is not None:
            excluded.to_excel(writer, index=False, sheet_name="Excluded")


def write_geojson(df: pd.DataFrame, path: str) -> None:
    subset = df.dropna(subset=["lat", "lon"])
    features = []
    for _, r in subset.iterrows():
        props = r.to_dict()
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(r["lon"]), float(r["lat"])]},
                "properties": props,
            }
        )
    geojson = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)


def write_folium_map(df: pd.DataFrame, path_html: str) -> None:
    subset = df.dropna(subset=["lat", "lon"])
    if len(subset):
        center = [subset["lat"].mean(), subset["lon"].mean()]
    else:
        center = [60.1699, 24.9384]  # Helsinki fallback
    m = folium.Map(location=center, zoom_start=9)
    for _, r in subset.iterrows():
        folium.Marker(
            [r["lat"], r["lon"]],
            popup=f"{r.get('name', '')} - {r.get('nearest_station', '')} ({r.get('distance_km', 0):.2f} km)",
        ).add_to(m)
    m.save(path_html)


def export_reports(df: pd.DataFrame, out_dir: str, excluded: Optional[pd.DataFrame] = None) -> None:
    """Write Excel/GeoJSON/HTML outputs."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    write_excel(df, str(out_path / "companies.xlsx"), excluded=excluded)
    write_geojson(df, str(out_path / "companies.geojson"))
    write_folium_map(df, str(out_path / "companies_map.html"))
