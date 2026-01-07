"""Distance helpers."""

from __future__ import annotations

import math
from typing import Iterable, Tuple


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance in kilometers."""
    radius = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * radius * math.asin(math.sqrt(a))


def nearest_station(
    lat: float, lon: float, stations: Iterable[Tuple[float, float]]
) -> Tuple[int, float]:
    """Return (index, distance_km) of nearest station from an iterable of (lat, lon)."""
    # Placeholder loop; optimize later if needed.
    best_idx = -1
    best_dist = float("inf")
    for idx, (slat, slon) in enumerate(stations):
        dist = haversine_km(lat, lon, slat, slon)
        if dist < best_dist:
            best_dist = dist
            best_idx = idx
    return best_idx, best_dist


def nearest_station_from_df(lat: float, lon: float, stations_df) -> Tuple[str, float]:
    """Return nearest station name and distance using a stations DataFrame."""
    coords = stations_df[["lat", "lon"]].to_numpy()
    idx, dist = nearest_station(lat, lon, coords)
    if idx == -1:
        return "", float("inf")
    name_col = "station_name" if "station_name" in stations_df.columns else None
    station_name = stations_df.iloc[idx][name_col] if name_col else ""
    return str(station_name), dist
