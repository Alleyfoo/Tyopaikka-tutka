#!/usr/bin/env python
"""Deprecated wrapper: use `python -m apprscan scan` instead."""

from __future__ import annotations

from apprscan.hiring_scan import main


if __name__ == "__main__":
    raise SystemExit(main())
