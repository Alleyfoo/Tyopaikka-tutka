import importlib.util
from pathlib import Path

import pandas as pd


def _load():
    """scripts/ has no __init__.py — load merge_companies.py directly."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "merge_companies.py"
    spec = importlib.util.spec_from_file_location("merge_companies", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _old():
    return pd.DataFrame(
        [
            {
                "business_id": "111111-1",
                "name": "Vanha Oy",
                "address": "Vanhatie 1",
                "website": "vanha.fi",
                "found_website": "https://vanha.fi",
                "website_status": "live",
                "llm_is_hiring": True,
            },
            {
                "business_id": "222222-2",
                "name": "Molemmissa Oy",
                "address": "Vanha katu 2",
                "website": "vanha2.fi",
                "found_website": "https://loytyi2.fi",
                "website_status": "live",
                "llm_is_hiring": False,
            },
        ]
    )


def _new():
    return pd.DataFrame(
        [
            {
                "business_id": "222222-2",
                "name": "Molemmissa Oy",
                "address": "Uusi katu 9",  # tuore PRH-osoite voittaa
                "website": "uusi2.fi",
            },
            {
                "business_id": "333333-3",
                "name": "Uusi Oy",
                "address": "Uusitie 3",
                "website": "uusi3.fi",
            },
        ]
    )


def test_old_only_rows_keep_additive_columns():
    mod = _load()
    merged, _ = mod.merge(_old(), _new())
    row = merged[merged["business_id"] == "111111-1"].iloc[0]
    assert row["name"] == "Vanha Oy"
    assert row["found_website"] == "https://vanha.fi"
    assert row["website_status"] == "live"
    assert bool(row["llm_is_hiring"]) is True


def test_both_rows_take_base_from_new_and_additive_from_old():
    mod = _load()
    merged, _ = mod.merge(_old(), _new())
    row = merged[merged["business_id"] == "222222-2"].iloc[0]
    assert row["address"] == "Uusi katu 9"  # base: uusi voittaa
    assert row["website"] == "uusi2.fi"
    assert row["found_website"] == "https://loytyi2.fi"  # additive: vanha säilyy
    assert row["website_status"] == "live"


def test_new_only_rows_get_additive_nan():
    mod = _load()
    merged, _ = mod.merge(_old(), _new())
    row = merged[merged["business_id"] == "333333-3"].iloc[0]
    assert row["name"] == "Uusi Oy"
    assert pd.isna(row["found_website"])
    assert pd.isna(row["llm_is_hiring"])


def test_empty_new_is_passthrough():
    mod = _load()
    old = _old()
    merged, _ = mod.merge(old, old.iloc[:0])
    assert len(merged) == len(old)
    assert list(merged["business_id"]) == list(old["business_id"])
    for col in ("found_website", "website_status", "llm_is_hiring"):
        assert merged[col].notna().sum() == old[col].notna().sum()


def test_survival_summary_counts():
    mod = _load()
    _, summary = mod.merge(_old(), _new())
    assert summary["found_website"] == {"old_nonnull": 2, "merged_nonnull": 2}
    assert summary["llm_is_hiring"] == {"old_nonnull": 2, "merged_nonnull": 2}


def test_default_preserve_excludes_build_schema():
    mod = _load()
    cols = ["name", "business_id", "address", "found_website", "llm_is_hiring"]
    assert mod.default_preserve(cols) == ["found_website", "llm_is_hiring"]
    # koko 13 sarakkeen build-skeema jää aina pois
    assert mod.default_preserve(mod.BUILD_SCHEMA) == []


def test_duplicate_business_id_raises():
    mod = _load()
    old = _old()
    dup = pd.concat([old, old.iloc[[1]]], ignore_index=True)  # sama bid kahdesti
    try:
        mod.merge(dup, _new())
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "yksikäsitteinen" in str(exc)
    try:
        mod.merge(_old(), pd.concat([_new(), _new().iloc[[1]]], ignore_index=True))
        raise AssertionError("expected ValueError")
    except ValueError as exc:
        assert "yksikäsitteinen" in str(exc)