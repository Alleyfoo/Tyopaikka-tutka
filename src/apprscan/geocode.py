"""Geocoding utilities with SQLite cache and Nominatim."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Tuple

from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter

DEFAULT_CACHE_PATH = Path("data/geocode_cache.sqlite")
GEOCODE_TIMEOUT = 10


def _ensure_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS geocode_cache (
            address TEXT PRIMARY KEY,
            lat REAL,
            lon REAL,
            ts TEXT
        )
        """
    )
    conn.commit()


def get_cached(address: str, cache_path: Path = DEFAULT_CACHE_PATH) -> Optional[Tuple[float, float]]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    try:
        _ensure_db(conn)
        cur = conn.execute("SELECT lat, lon FROM geocode_cache WHERE address = ?", (address,))
        row = cur.fetchone()
        if row is None:
            return None
        return float(row[0]), float(row[1])
    finally:
        conn.close()


def set_cached(address: str, lat: float, lon: float, cache_path: Path = DEFAULT_CACHE_PATH) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(cache_path)
    try:
        _ensure_db(conn)
        conn.execute(
            "INSERT OR REPLACE INTO geocode_cache(address, lat, lon, ts) VALUES (?, ?, ?, ?)",
            (address, lat, lon, datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _build_geocoder() -> Callable[[str], Optional[object]]:
    nominatim = Nominatim(user_agent="apprenticeship-employer-scanner", timeout=GEOCODE_TIMEOUT)
    return RateLimiter(
        nominatim.geocode,
        min_delay_seconds=1.1,
        swallow_exceptions=True,
        max_retries=2,
        error_wait_seconds=2.0,
    )


def geocode_address(
    address: str,
    *,
    cache_path: Path = DEFAULT_CACHE_PATH,
    geocoder: Optional[Callable[[str], Optional[object]]] = None,
) -> Tuple[Optional[float], Optional[float], str, bool]:
    """Return (lat, lon, provider, cached_bool)."""
    cached = get_cached(address, cache_path)
    if cached:
        return cached[0], cached[1], "cache", True

    geocode_func = geocoder or _build_geocoder()
    try:
        loc = geocode_func(f"{address}, Finland")
    except Exception:
        return None, None, "nominatim_error", False

    if loc is None:
        return None, None, "nominatim", False

    lat, lon = float(loc.latitude), float(loc.longitude)
    set_cached(address, lat, lon, cache_path)
    return lat, lon, "nominatim", False
