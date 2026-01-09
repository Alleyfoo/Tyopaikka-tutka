"""Repo health checks for apprscan."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import requests

from . import __version__
from .hiring_scan import PROMPT_VERSION, _load_env_file, evaluate_html
from .output_contract import validate_hiring_signal_rows


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _resolve_env(env_file: Path | None) -> dict[str, str]:
    env = _load_env_file(env_file) if env_file else {}
    merged = dict(env)
    for key in ("OLLAMA_HOST", "OLLAMA_URL", "MODEL_NAME", "OLLAMA_MODEL"):
        if key in os.environ:
            merged[key] = os.environ[key]
    return merged


def _default_env_file(env_file: Path | None) -> Path | None:
    if env_file:
        return env_file
    repo_root = Path(__file__).resolve().parents[2]
    candidate = repo_root / ".env"
    return candidate if candidate.exists() else None


def check_ollama(env_file: Path | None) -> List[str]:
    env = _resolve_env(env_file)
    host = env.get("OLLAMA_URL") or env.get("OLLAMA_HOST") or "http://127.0.0.1:11434"
    if "ollama:11434" in host:
        host = "http://127.0.0.1:11434"
    model = env.get("MODEL_NAME") or env.get("OLLAMA_MODEL") or ""
    errors: List[str] = []
    if not model:
        errors.append("OLLAMA_MODEL not set")
    try:
        resp = requests.get(host.rstrip("/") + "/api/tags", timeout=5)
        if resp.status_code >= 400:
            errors.append(f"Ollama unreachable (HTTP {resp.status_code})")
        else:
            payload = resp.json()
            names = [m.get("name") for m in payload.get("models", []) if isinstance(m, dict)]
            if model and model not in names:
                errors.append(f"Ollama model not found: {model}")
    except Exception as exc:
        errors.append(f"Ollama unreachable: {exc}")
    return errors


def check_cache_dirs() -> List[str]:
    errors: List[str] = []
    for path in [Path("out"), Path("data")]:
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            errors.append(f"Cannot create {path}: {exc}")
    return errors


def run_pytest() -> int:
    cmd = [sys.executable, "-m", "pytest", "-q"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
    return result.returncode


def run_fixture_smoke(fixtures_dir: Path) -> List[str]:
    labels_path = fixtures_dir / "labels.json"
    if not labels_path.exists():
        return [f"Missing labels: {labels_path}"]
    labels = json.loads(labels_path.read_text(encoding="utf-8"))
    rows = []
    for idx, item in enumerate(labels, start=1):
        file_name = item.get("file")
        if not file_name:
            continue
        html = (fixtures_dir / file_name).read_text(encoding="utf-8")
        url = f"https://example.com/{file_name}"
        result = evaluate_html(html, url=url)
        rows.append(
            {
                "run_id": "fixture",
                "tool_version": __version__,
                "git_sha": "",
                "crawl_ts": _now_iso(),
                "station": "fixture",
                "max_distance_km": 0.0,
                "business_id": f"fixture_{idx}",
                "name": f"fixture_{idx}",
                "domain": "example.com",
                "signal": str(result.get("signal") or "").lower(),
                "confidence": result.get("confidence") or 0.0,
                "evidence": result.get("evidence") or "",
                "evidence_snippets": result.get("evidence_snippets") or [],
                "evidence_urls": result.get("evidence_urls") or [url],
                "signal_url": url,
                "checked_urls": url,
                "next_url_hint": "",
                "errors": "",
                "skipped_reason": "",
                "ollama_model": "",
                "ollama_temperature": 0.0,
                "prompt_version": PROMPT_VERSION,
                "llm_used": False,
                "output_format": "jsonl",
            }
        )
    return validate_hiring_signal_rows(rows)


def run_checks(env_file: Path | None) -> int:
    print("== apprscan check ==")
    errors: List[str] = []

    env_file = _default_env_file(env_file)

    print("[1/4] pytest")
    if run_pytest() != 0:
        errors.append("pytest failed")

    print("[2/4] fixture smoke scan + schema validation")
    errors.extend(run_fixture_smoke(Path("tests/fixtures/hiring_signal")))

    print("[3/4] output directories")
    errors.extend(check_cache_dirs())

    print("[4/4] ollama sanity")
    errors.extend(check_ollama(env_file))

    if errors:
        print("Check failed:")
        for err in errors:
            print(f"- {err}")
        return 1
    print("All checks passed.")
    return 0
