#!/usr/bin/env python
"""Check that package version is consistent across files."""

from __future__ import annotations

import re
from pathlib import Path


def _read_pyproject_version(path: Path) -> str | None:
    text = path.read_text(encoding="utf-8")
    match = re.search(r'(?m)^version\\s*=\\s*"([^"]+)"\\s*$', text)
    if not match:
        return None
    return match.group(1).strip()


def _read_init_version(path: Path) -> str | None:
    text = path.read_text(encoding="utf-8")
    match = re.search(r'__version__\\s*=\\s*"([^"]+)"', text)
    if not match:
        return None
    return match.group(1).strip()


def _read_changelog_version(path: Path) -> str | None:
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line.startswith("##"):
            continue
        match = re.match(r"##\\s+v?([0-9]+\\.[0-9]+\\.[0-9]+)", line)
        if match:
            return match.group(1)
    return None


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    pyproject = root / "pyproject.toml"
    init_py = root / "src" / "apprscan" / "__init__.py"
    changelog = root / "CHANGELOG.md"

    errors = []
    pyproject_version = _read_pyproject_version(pyproject)
    init_version = _read_init_version(init_py)
    changelog_version = _read_changelog_version(changelog)

    if not pyproject_version:
        errors.append("pyproject.toml version not found")
    if not init_version:
        errors.append("__init__.py version not found")
    if not changelog_version:
        errors.append("CHANGELOG.md version not found")

    if pyproject_version and init_version and pyproject_version != init_version:
        errors.append(f"pyproject.toml ({pyproject_version}) != __init__.py ({init_version})")
    if pyproject_version and changelog_version and pyproject_version != changelog_version:
        errors.append(f"pyproject.toml ({pyproject_version}) != CHANGELOG.md ({changelog_version})")

    if errors:
        print("Version check failed:")
        for err in errors:
            print(f"- {err}")
        return 1

    print(f"Version check OK: {pyproject_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
