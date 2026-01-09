"""Output contract validation for hiring signal scans."""

from __future__ import annotations

import json
from typing import Iterable, List, Mapping


REQUIRED_COLUMNS = [
    "run_id",
    "tool_version",
    "git_sha",
    "crawl_ts",
    "station",
    "max_distance_km",
    "business_id",
    "name",
    "domain",
    "signal",
    "confidence",
    "evidence",
    "evidence_snippets",
    "evidence_urls",
    "signal_url",
    "checked_urls",
    "next_url_hint",
    "errors",
    "skipped_reason",
    "ollama_model",
    "ollama_temperature",
    "prompt_version",
    "llm_used",
    "output_format",
]

LIST_COLUMNS = {"evidence_snippets", "evidence_urls"}
ENUM_SIGNALS = {"yes", "no", "unclear"}
ENUM_FORMATS = {"csv", "jsonl"}
BOOL_TRUE = {"true", "1", "yes", "y", "t"}
BOOL_FALSE = {"false", "0", "no", "n", "f"}


def _parse_list(val) -> list[str] | None:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(v) for v in val]
    if isinstance(val, str):
        raw = val.strip()
        if raw == "":
            return []
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
            return None
        # CSV fallback: split on semicolon
        return [seg.strip() for seg in raw.split(";") if seg.strip()]
    return None


def _parse_bool(val) -> bool | None:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)) and val in (0, 1):
        return bool(val)
    if isinstance(val, str):
        lowered = val.strip().lower()
        if lowered in BOOL_TRUE:
            return True
        if lowered in BOOL_FALSE:
            return False
    return None


def validate_hiring_signal_rows(rows: Iterable[Mapping]) -> List[str]:
    errors: List[str] = []
    for idx, row in enumerate(rows, start=1):
        for col in REQUIRED_COLUMNS:
            if col not in row:
                errors.append(f"row {idx}: missing column {col}")
        signal = str(row.get("signal") or "").lower()
        if signal and signal not in ENUM_SIGNALS:
            errors.append(f"row {idx}: invalid signal {signal}")
        output_format = str(row.get("output_format") or "").lower()
        if output_format and output_format not in ENUM_FORMATS:
            errors.append(f"row {idx}: invalid output_format {output_format}")
        for col in LIST_COLUMNS:
            parsed = _parse_list(row.get(col))
            if parsed is None:
                errors.append(f"row {idx}: invalid list column {col}")
        if "deterministic" in row and row.get("deterministic") is not None:
            deterministic = _parse_bool(row.get("deterministic"))
            if deterministic is None:
                errors.append(f"row {idx}: invalid deterministic")
        llm_used = _parse_bool(row.get("llm_used"))
        if llm_used is None:
            errors.append(f"row {idx}: invalid llm_used")
        try:
            conf = float(row.get("confidence") or 0.0)
            if conf < 0 or conf > 1:
                errors.append(f"row {idx}: confidence out of range {conf}")
        except (TypeError, ValueError):
            errors.append(f"row {idx}: invalid confidence")
        try:
            float(row.get("ollama_temperature") or 0.0)
        except (TypeError, ValueError):
            errors.append(f"row {idx}: invalid ollama_temperature")
        try:
            float(row.get("max_distance_km") or 0.0)
        except (TypeError, ValueError):
            errors.append(f"row {idx}: invalid max_distance_km")
    return errors
