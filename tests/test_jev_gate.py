import pandas as pd

from apprscan import jev_gate
from apprscan.jev_gate import GateDecision, classify, decide


def _row(**overrides):
    row = {
        "business_id": "1",
        "website_status": "live",
        "llm_is_hiring": None,
        "e_industry": 4,
        "e_commute": 3,
        "e_hiring": 1,
        "e_fit": 8,
        "best_website": "https://example.com",
    }
    row.update(overrides)
    return row


def _frame(rows):
    return pd.DataFrame(rows)


def test_decide_tier_c_no_live_website():
    d = decide(_row(website_status="", e_fit=8))
    assert d.tier == "C"
    assert "verkkosivua" in d.reason
    assert d.gate_model == "rules"


def test_decide_tier_c_no_fit_anywhere():
    d = decide(_row(e_industry=0, e_commute=0))
    assert d.tier == "C"
    assert "sovi" in d.reason


def test_decide_tier_a_hiring_signal():
    d = decide(_row(llm_is_hiring="True"))
    assert d.tier == "A"
    assert "Rekrytointisignaali" in d.reason


def test_decide_tier_a_strong_industry():
    d = decide(_row(e_industry=4, e_commute=2))
    assert d.tier == "A"
    assert "Sopiva ala" in d.reason


def test_decide_tier_b_long_commute():
    # hyva ala mutta liian pitka matka -> B
    d = decide(_row(e_industry=4, e_commute=1))
    assert d.tier == "B"


def test_decide_tier_b_weak_industry():
    # lyhyt matka mutta heikko ala eika rekrysignalia -> B
    d = decide(_row(e_industry=1, e_commute=3))
    assert d.tier == "B"


def test_decide_never_raises_on_junk():
    class Junk:
        def get(self, key, default=None):
            raise RuntimeError("boom")

    d = decide(Junk())
    assert d.tier == "B"
    assert isinstance(d, GateDecision)


def test_classify_adds_columns():
    out = classify(_frame([_row(), _row(business_id="2", e_commute=1)]))
    assert set(["tier", "gate_reason", "gate_model"]).issubset(out.columns)
    assert list(out["tier"]) == ["A", "B"]
    assert out["gate_model"].eq("rules").all()


def test_classify_empty_frame():
    out = classify(pd.DataFrame(columns=["business_id"]))
    assert list(out.columns) == ["business_id", "tier", "gate_reason", "gate_model"]


def test_deep_check_promotes_b_to_a(monkeypatch):
    html = "<html><body>Join us — open positions available now!</body></html>"
    monkeypatch.setattr(jev_gate, "_fetch_html", lambda url, timeout=10.0: html)
    # B-rivi: hyva ala mutta liian pitka matka
    out = classify(_frame([_row(e_commute=1)]), deep_check=1)
    assert out["tier"].iloc[0] == "A"
    assert "B->A" in out["gate_reason"].iloc[0]
    assert out["gate_model"].iloc[0] == "rules+heuristiikka"


def test_deep_check_stays_b_without_signal(monkeypatch):
    html = "<html><body>We sell bolts and screws wholesale.</body></html>"
    monkeypatch.setattr(jev_gate, "_fetch_html", lambda url, timeout=10.0: html)
    out = classify(_frame([_row(e_commute=1)]), deep_check=1)
    assert out["tier"].iloc[0] == "B"
    assert out["gate_model"].iloc[0] == "rules"


def test_unreachable_model_host_never_raises():
    # Ollama ei kaynnissa / vaara port -> rivi jaa B:ksi, ei poikkeusta
    out = classify(
        _frame([_row(e_commute=1)]),
        deep_check=1,
        model="fake-model",
        host="http://localhost:1",
    )
    assert out["tier"].iloc[0] == "B"
    assert "myöhemmin" in out["gate_reason"].iloc[0]