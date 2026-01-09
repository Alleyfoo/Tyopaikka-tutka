# Hiring signal output contract

This document defines the schema for hiring-signal scan outputs produced by `apprscan scan`.
The authoritative machine-readable schema lives at `schemas/hiring_signal_output.schema.json`.

## Formats
- CSV: list columns (`evidence_snippets`, `evidence_urls`) are JSON-encoded arrays.
- JSONL: one JSON object per line with arrays kept as arrays.

## Required columns
- `run_id`: run identifier (string).
- `tool_version`: apprscan version string.
- `git_sha`: short git SHA when available (string, may be empty).
- `crawl_ts`: UTC ISO-8601 timestamp for the scan run.
- `station`: station filter label used for the scan.
- `max_distance_km`: numeric distance filter used for the scan.
- `business_id`: company identifier from the master file.
- `name`: company name.
- `domain`: resolved domain used for the scan.
- `signal`: `yes`, `no`, or `unclear`.
- `confidence`: 0.0-1.0.
- `evidence`: short reason string.
- `evidence_snippets`: list of 2-6 short text snippets when `signal` is `yes`/`no`.
- `evidence_urls`: list of URLs supporting the evidence.
- `signal_url`: URL where the primary signal was detected.
- `checked_urls`: semicolon-separated list of URLs checked.
- `next_url_hint`: optional next URL to probe if unclear.
- `errors`: semicolon-separated error strings.
- `skipped_reason`: semicolon-separated skip reasons (robots, fetch, etc).
- `ollama_model`: model name used (string).
- `ollama_temperature`: numeric temperature used.
- `prompt_version`: hash of the system prompt.
- `llm_used`: boolean indicating if LLM fallback was used.
- `output_format`: `csv` or `jsonl`.

## Optional columns
- `deterministic`: boolean indicating `--deterministic` mode (temperature forced to 0).

## Notes
- If evidence snippets or URLs are missing for `yes`/`no`, the scan downgrades to `unclear`.
- Downstream tooling should treat unknown columns as additive and ignore them safely.
