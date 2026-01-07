"""Scoring utilities."""

from __future__ import annotations

from typing import Any, Dict, Tuple


def score_company(company: Dict[str, Any]) -> Tuple[float, str]:
    """Return (score, reasons_text)."""
    raise NotImplementedError("Scoring not implemented yet.")

