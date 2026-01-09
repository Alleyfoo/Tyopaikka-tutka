import json
from pathlib import Path

from apprscan.output_contract import REQUIRED_COLUMNS, validate_hiring_signal_rows


def test_hiring_signal_output_contract_valid_rows():
    rows = [
        {
            "run_id": "fixture",
            "tool_version": "0.6.0",
            "git_sha": "abc123",
            "crawl_ts": "2026-01-01T00:00:00Z",
            "station": "Lahti",
            "max_distance_km": 1.0,
            "business_id": "123",
            "name": "Test Oy",
            "domain": "example.com",
            "signal": "yes",
            "confidence": 0.8,
            "evidence": "careers",
            "evidence_snippets": ["We are hiring", "Open positions"],
            "evidence_urls": ["https://example.com/careers"],
            "signal_url": "https://example.com/careers",
            "checked_urls": "https://example.com;https://example.com/careers",
            "next_url_hint": "",
            "errors": "",
            "skipped_reason": "",
            "ollama_model": "llama3",
            "ollama_temperature": 0.2,
            "prompt_version": "deadbeef",
            "deterministic": False,
            "llm_used": True,
            "output_format": "jsonl",
        },
        {
            "run_id": "fixture",
            "tool_version": "0.6.0",
            "git_sha": "",
            "crawl_ts": "2026-01-01T00:00:00Z",
            "station": "Lahti",
            "max_distance_km": "1.0",
            "business_id": "456",
            "name": "Example Oy",
            "domain": "example.org",
            "signal": "unclear",
            "confidence": "0.2",
            "evidence": "",
            "evidence_snippets": "[]",
            "evidence_urls": "[\"https://example.org\"]",
            "signal_url": "https://example.org",
            "checked_urls": "https://example.org",
            "next_url_hint": "",
            "errors": "",
            "skipped_reason": "",
            "ollama_model": "",
            "ollama_temperature": "0.0",
            "prompt_version": "deadbeef",
            "llm_used": "false",
            "output_format": "csv",
        },
    ]
    errors = validate_hiring_signal_rows(rows)
    assert errors == []


def test_output_contract_schema_matches_required_columns():
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "schemas" / "hiring_signal_output.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    assert set(schema["required"]) == set(REQUIRED_COLUMNS)
