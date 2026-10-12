"""Jev-style decision gate — cheap triage before anything reaches the big LLM.

A small decision layer that sits alongside the analysis pipeline: it sorts
companies into tiers (A = analysoi nyt, B = myöhemmin, C = ohita) from
structured inputs only (website_status, llm_is_hiring, e_* scores), logs a
Finnish one-line rationale for every decision, and never raises.

Rules are exact, free and auditable. The optional deep check (``deep_check``)
looks at the top-N Tier-B homepages with the existing hiring heuristics
(``contains_job_signal``, ATS detectors, JSON-LD JobPosting) and can promote
them to A; an optional small Ollama model (``model``) gets one cheap
/api/generate JSON question for the rows the heuristics could not decide.

Nothing here depends on a pullable tiny model — the gate is fully functional
on rules + heuristics alone; ``--jev-model`` is additive.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Optional

import httpx
import pandas as pd

from .domains_discovery import contains_job_signal
from .employer_fit import _truthy
from .jobs.ats import detect_ats

TIERS = ("A", "B", "C")
DEFAULT_HOST = "http://localhost:11434"


@dataclass(frozen=True)
class GateDecision:
    """One tier decision with its rationale and which gate layer made it."""

    tier: str  # "A" | "B" | "C"
    reason: str  # Finnish one-liner
    gate_model: str = "rules"  # "rules" | "rules+heuristiikka" | deep-check model name


def _int(value) -> int:
    """NaN-tolerant int coercion for e_* score cells."""
    try:
        if value is None or pd.isna(value):
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def decide(row) -> GateDecision:
    """Tier for one company from structured inputs. Pure, never raises.

    C (skip):  no live website, or zero fit on both industry and commute.
    A (analyze): live, short commute, and either a detected hiring signal
                 or a strongly fitting industry.
    B (later):  everything else with a live website.
    """
    try:
        status = str(row.get("website_status", "") or "").strip().lower()
        e_industry = _int(row.get("e_industry"))
        e_commute = _int(row.get("e_commute"))
        hiring = _truthy(row.get("llm_is_hiring", None))

        if status != "live":
            return GateDecision("C", "Ei elävää verkkosivua — ei analysoitavaa")
        if e_industry == 0 and e_commute == 0:
            return GateDecision("C", "Ei sovi alalta eikä työmatkalta")
        if e_commute >= 2 and (hiring or e_industry >= 2):
            if hiring:
                return GateDecision("A", "Rekrytointisignaali + kohtuullinen työmatka")
            return GateDecision("A", "Sopiva ala + kohtuullinen työmatka")
        return GateDecision("B", "Keskiverto potentiaali — tarkista myöhemmin")
    except Exception:
        # A gate that crashes is worse than a gate that says "later".
        return GateDecision("B", "Portti ei saanut tietoja — oletus myöhemmin")


def _fetch_html(url: str, timeout: float = 10.0) -> Optional[str]:
    """Fetch one homepage; None on any failure (mirrors analyze_websites)."""
    try:
        target = url if "://" in url else f"https://{url}"
        resp = httpx.get(
            target,
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; TyopaikkaSkanneri/1.0)"},
        )
        resp.raise_for_status()
        return resp.text
    except Exception:
        return None


def _model_ask(host: str, model: str, html: str, timeout: float = 60.0) -> Optional[bool]:
    """One cheap JSON question to a small local model; None on any failure."""
    try:
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)[:2000]
        payload = {
            "model": model,
            "stream": False,
            "format": "json",
            "prompt": (
                "Alla on yrityksen verkkosivun teksti. Vastaako sivu rekrytointiin "
                "(avoimet työpaikat, career-sivu, hakemusohjeet)?\n"
                'Vastaa pelkästään JSON-muodossa {"hiring": true} tai {"hiring": false}.\n\n'
                f"Sivun teksti:\n{text}"
            ),
        }
        resp = httpx.post(f"{host}/api/generate", json=payload, timeout=timeout)
        resp.raise_for_status()
        answer = json.loads(resp.json().get("response", "{}"))
        return bool(answer.get("hiring"))
    except Exception:
        return None


def _deep_check_top_b(
    out: pd.DataFrame, *, deep_check: int, model: Optional[str], host: str
) -> None:
    """Promote the most promising Tier-B rows using homepage evidence.

    Heuristics first (free, exact); the model only gets the rows the
    heuristics could not decide. Any failure leaves the row at B.
    """
    b_idx = out.index[out["tier"] == "B"]
    if len(b_idx) == 0:
        return
    ranked = out.loc[b_idx].sort_values("e_fit", ascending=False).head(deep_check)
    for idx in ranked.index:
        try:
            row = out.loc[idx]
            url = str(row.get("best_website") or row.get("website") or "").strip()
            if not url:
                continue
            html = _fetch_html(url)
            if not html:
                continue
            url_for_ats = url if "://" in url else f"https://{url}"
            hit = (
                contains_job_signal(html)
                or detect_ats(url_for_ats, html) is not None
                or "jobposting" in html.lower()
            )
            if hit:
                out.at[idx, "tier"] = "A"
                out.at[idx, "gate_reason"] = "B->A: rekrytointisignaali sivulla (heuristiikka)"
                out.at[idx, "gate_model"] = "rules+heuristiikka"
                continue
            if model:
                hiring = _model_ask(host, model, html)
                if hiring is True:
                    out.at[idx, "tier"] = "A"
                    out.at[idx, "gate_reason"] = f"B->A: rekrytointisignaali mallin mukaan ({model})"
                    out.at[idx, "gate_model"] = model
                elif hiring is False:
                    out.at[idx, "gate_reason"] = f"B: ei rekrytointisignaalia ({model})"
                    out.at[idx, "gate_model"] = model
        except Exception:
            # Deep check must never take the whole classification down.
            continue


def classify(
    frame: pd.DataFrame,
    *,
    deep_check: int = 0,
    model: Optional[str] = None,
    host: str = DEFAULT_HOST,
) -> pd.DataFrame:
    """Add tier / gate_reason / gate_model columns to a scored frame.

    ``deep_check`` > 0: that many most-promising Tier-B rows get a homepage
    deep check (heuristics, then the optional model). Never raises.
    """
    out = frame.copy()
    if len(out) == 0:
        out["tier"] = []
        out["gate_reason"] = []
        out["gate_model"] = []
        return out
    decisions = out.apply(decide, axis=1)
    out["tier"] = [d.tier for d in decisions]
    out["gate_reason"] = [d.reason for d in decisions]
    out["gate_model"] = [d.gate_model for d in decisions]

    if deep_check > 0:
        _deep_check_top_b(out, deep_check=deep_check, model=model, host=host)
    return out