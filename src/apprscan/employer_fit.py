"""Employer-fit scoring — how well a company works as an employer for the
job seeker, not as a sales lead (that is :mod:`apprscan.leads`).

Score 0-10 on three axes: TOI industry fit (does this industry hire IT
support / data people), hiring signal (LLM-detected recruitment or at
least a live website), and door-to-door commute from Mäntsälä.

Pure functions only, same shape as leads.py: prefix dicts + additive
integer axes + one ``add_*_scores(frame)`` entry point. No IO, no
sidebar dependency — usable from the dashboard, the jev gate and the
training script alike.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from . import transit
from .leads import prefix_score

# TOI prefix -> 0-4. Industries that employ IT support and data people.
# Convention (as in leads.py): a longer prefix must score >= its shorter
# parent, because prefix_score takes the max over all matching prefixes.
INDUSTRY_FIT = {
    # Ohjelmistot ja IT-palvelut (62xxx, 63xxx)
    "62": 4, "63": 4,
    # Tietoliikenne ja julkaisu
    "61": 3, "58": 2,
    # Henkilöstöpalvelut — IT-vuokrauskanava
    "78": 3,
    # Rahoitus — dataintensiivistä
    "64": 2, "65": 2, "66": 2,
    # Muut digitaaliset / toimistopainotteiset
    "70": 1, "71": 1, "72": 1, "73": 1, "74": 1, "85": 1,
}

# Door-to-door commute bands in minutes: <=35 -> 3, <=50 -> 2, <=70 -> 1, else 0.
# With the dashboard defaults (bus last mile, +10 min overhead) this yields
# roughly: Kerava ~31 -> 3, Järvenpää ~37 -> 2, Pasila ~46 -> 2,
# Hyvinkää ~52 -> 1, Riihimäki ~60 -> 1, Helsinki ~55+ -> 1.
COMMUTE_BANDS_MIN = (35.0, 50.0, 70.0)

EMPLOYER_AXES = {"Alan sopivuus": "e_industry", "Rekrytointi": "e_hiring", "Työmatka": "e_commute"}
EMPLOYER_SCORE_MAX = 10  # e_industry (4) + e_hiring (3) + e_commute (3)


def _truthy(value) -> bool:
    """Coerce object-dtype values (True/False/'true'/'1'/nan) to a bool."""
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "kyllä")
    return bool(value)


def industry_score(toi) -> int:
    """0-4: how strongly this industry hires IT support / data people."""
    return prefix_score(toi, INDUSTRY_FIT)


def hiring_score(is_hiring, website_status) -> int:
    """0-3: LLM-detected recruitment beats a merely live website."""
    if _truthy(is_hiring):
        return 3
    if website_status == "live":
        return 1
    return 0


def commute_score(minutes) -> int:
    """0-3 from door-to-door minutes; missing data scores 0."""
    if minutes is None or pd.isna(minutes):
        return 0
    if minutes <= COMMUTE_BANDS_MIN[0]:
        return 3
    if minutes <= COMMUTE_BANDS_MIN[1]:
        return 2
    if minutes <= COMMUTE_BANDS_MIN[2]:
        return 1
    return 0


def add_employer_scores(
    frame: pd.DataFrame,
    *,
    origin: str = transit.DEFAULT_ORIGIN,
    mode: str = "bus",
    overhead_min: float = 10.0,
    rail_minutes: Optional[dict] = None,
) -> pd.DataFrame:
    """Add e_industry / e_hiring / e_commute / e_fit (0-10) to a frame.

    If the frame already carries a ``commute_min`` column (the dashboard
    computes one from the sidebar's origin/mode/overhead), it is used as-is;
    otherwise commute is computed here with the given defaults.
    """
    out = frame.copy()
    out["e_industry"] = out["toi_code"].apply(industry_score)
    out["e_hiring"] = out.apply(
        lambda r: hiring_score(
            r["llm_is_hiring"] if "llm_is_hiring" in out.columns else None,
            r["website_status"] if "website_status" in out.columns else None,
        ),
        axis=1,
    )

    if "commute_min" in out.columns:
        minutes = out["commute_min"]
    else:
        rail = rail_minutes if rail_minutes is not None else transit.rail_minutes_from(origin)

        def _minutes(row) -> float:
            distance = row["dist_km"] if "dist_km" in out.columns else row.get("distance_km")
            value = transit.commute_minutes(
                row["nearest_station"],
                distance,
                origin=origin,
                mode=mode,
                rail_minutes=rail,
                overhead_min=overhead_min,
            )
            return value if value is not None else float("nan")

        minutes = out.apply(_minutes, axis=1)

    out["e_commute"] = minutes.apply(commute_score)
    out["e_fit"] = out["e_industry"] + out["e_hiring"] + out["e_commute"]
    return out