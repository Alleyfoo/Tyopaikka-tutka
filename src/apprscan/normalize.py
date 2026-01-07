"""Data normalization helpers for PRH rows."""

from __future__ import annotations

from typing import Iterable, List, Mapping, Any

import pandas as pd
from unidecode import unidecode


ADDR_CANDIDATES = {
    "street": ["addresses.0.street", "street"],
    "postCode": ["addresses.0.postCode", "postCode"],
    "city": ["addresses.0.city", "city", "domicile", "_source_city"],
}


def pick_first(row: Mapping[str, Any], candidates: List[str]) -> str:
    for col in candidates:
        if col in row and pd.notna(row[col]) and str(row[col]).strip():
            return str(row[col]).strip()
    return ""


def clean_address(street: str, post_code: str, city: str) -> str:
    parts = [p.strip() for p in [street or "", post_code or "", city or ""] if p and p.strip()]
    return unidecode(", ".join(parts))


def normalize_companies(rows: Iterable[dict]) -> pd.DataFrame:
    rows_list = list(rows)
    df = pd.json_normalize(rows_list, sep=".")
    if df.empty:
        df["full_address"] = []
        return df

    streets = []
    posts = []
    cities = []
    for idx, row_series in df.iterrows():
        row = row_series.to_dict()
        original = rows_list[idx]
        addresses = original.get("addresses") if isinstance(original, dict) else None
        if isinstance(addresses, list) and addresses:
            first_addr = addresses[0] or {}
            for key, value in first_addr.items():
                row[f"addresses.0.{key}"] = value

        street = pick_first(row, ADDR_CANDIDATES["street"])
        post = pick_first(row, ADDR_CANDIDATES["postCode"])
        city = pick_first(row, ADDR_CANDIDATES["city"])
        streets.append(street)
        posts.append(post)
        cities.append(city)

    df["full_address"] = [
        clean_address(street, post, city) for street, post, city in zip(streets, posts, cities)
    ]
    return df
