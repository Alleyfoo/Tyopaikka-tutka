"""Filtering utilities for company rows."""

from __future__ import annotations

from typing import Any, Dict, Tuple


def exclude_company(company: Dict[str, Any]) -> Tuple[bool, str]:
    """Return (excluded_bool, reason)."""
    raise NotImplementedError("Filter logic not implemented yet.")

