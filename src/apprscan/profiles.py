"""Profile loading and merging."""

from __future__ import annotations

import yaml
from pathlib import Path
from typing import Dict, Any

DEFAULT_PROFILE_PATH = Path("config/profiles.yaml")


def load_profiles(path: str | Path | None = None) -> Dict[str, Dict[str, Any]]:
    profiles_path = Path(path or DEFAULT_PROFILE_PATH)
    if not profiles_path.exists():
        return {}
    with profiles_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    profiles: Dict[str, Dict[str, Any]] = {}
    for name, cfg in data.items():
        if isinstance(cfg, dict):
            profiles[name] = cfg
    return profiles


def apply_profile(profile_name: str, profiles: Dict[str, Dict[str, Any]], args: Dict[str, Any]) -> Dict[str, Any]:
    if not profile_name or profile_name not in profiles:
        return args
    merged = profiles[profile_name].copy()
    merged.update({k: v for k, v in args.items() if v not in (None, "", False)})
    return merged
