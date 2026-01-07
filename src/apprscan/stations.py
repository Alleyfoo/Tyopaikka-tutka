"""Station data handling with local-first strategy."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

TRAINLINE_URL = "https://raw.githubusercontent.com/trainline-eu/stations/master/stations.csv"
DEFAULT_LOCAL = Path("data/stations_fi.csv")


def _read_local(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def _read_remote() -> pd.DataFrame:
    resp = requests.get(TRAINLINE_URL, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), sep=";")


def load_stations(use_local: bool = True, path: Optional[str | Path] = None) -> pd.DataFrame:
    """Load station data, preferring a curated local CSV when available."""
    local_path = Path(path) if path else DEFAULT_LOCAL

    df = None
    if use_local and local_path.exists():
        df = _read_local(local_path)
    else:
        df = _read_remote()

    # Standardize columns
    if "country" in df.columns:
        df = df[df["country"] == "FI"] if "FI" in df["country"].unique() else df

    rename_map = {"latitude": "lat", "longitude": "lon", "name": "station_name"}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    keep_cols = [c for c in ["station_name", "lat", "lon", "uic", "city_name", "country"] if c in df.columns]
    df = df[keep_cols]
    df = df.dropna(subset=["lat", "lon"])
    return df.reset_index(drop=True)
