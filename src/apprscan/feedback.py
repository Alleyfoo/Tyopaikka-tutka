"""Feedback store for employer-fit learning.

Simple local sqlite store for the user's per-company curation clicks
(kiinnostaa / ei_kiinnosta / hakenee) made in the dashboard. Two tables:
``feedback`` holds the current label per business_id (idempotent overwrite),
``feedback_log`` keeps an append-only history of every recording.

Local runs only — the deployed Streamlit Cloud copy has no writable path,
so the tab notes this and stays read-only there.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = _ROOT / "data" / "feedback.sqlite"

# Valid labels. "hakenee" (applied) counts as positive for training,
# same as "kiinnostaa"; "ei_kiinnosta" is the negative.
LABELS = ("kiinnostaa", "ei_kiinnosta", "hakenee")


def connect(path=DEFAULT_DB) -> sqlite3.Connection:
    """Open (creating if needed) the feedback database."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback (
            business_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'dashboard',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feedback_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            label TEXT NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def record(conn: sqlite3.Connection, business_id: str, label: str, source: str = "dashboard") -> None:
    """Set the current label for one company and append to the history log.

    Idempotent: re-recording the same label replaces the feedback row
    (same business_id) while the log keeps every event.
    """
    if label not in LABELS:
        raise ValueError(f"Unknown feedback label: {label!r} (expected one of {LABELS})")
    now = pd.Timestamp.utcnow().isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO feedback (business_id, label, source, updated_at) VALUES (?, ?, ?, ?)",
        (str(business_id), label, source, now),
    )
    conn.execute(
        "INSERT INTO feedback_log (business_id, label, source, created_at) VALUES (?, ?, ?, ?)",
        (str(business_id), label, source, now),
    )
    conn.commit()


def load(conn: sqlite3.Connection) -> pd.DataFrame:
    """Current labels as a frame: business_id, label, source, updated_at."""
    return pd.read_sql_query(
        "SELECT business_id, label, source, updated_at FROM feedback", conn
    )


def load_log(conn: sqlite3.Connection) -> pd.DataFrame:
    """Full append-only history: business_id, label, source, created_at."""
    return pd.read_sql_query(
        "SELECT business_id, label, source, created_at FROM feedback_log ORDER BY id", conn
    )