import importlib.util
from pathlib import Path

import pandas as pd


def _load():
    """scripts/ has no __init__.py — load train_fit_model.py directly."""
    path = Path(__file__).resolve().parents[1] / "scripts" / "train_fit_model.py"
    spec = importlib.util.spec_from_file_location("train_fit_model", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _companies(n=4):
    return pd.DataFrame(
        [
            {
                "business_id": f"10000{i}-1",
                "name": f"Yritys {i}",
                "industry": "Ohjelmistot",
                "toi_code": "62010",
                "nearest_station": "Kerava",
                "commute_min": 31.0,
                "website_status": "live",
                "website": "esimerkki.fi",
                "distance_km": 1.0 + i,
                "registered": "2015-02-01",
                "llm_is_hiring": True if i % 2 else None,
                "llm_has_shop": False,
                "llm_has_online_sales": False,
            }
            for i in range(n)
        ]
    )


def _labels():
    return pd.DataFrame(
        [
            {"business_id": "100000-1", "label": "kiinnostaa"},
            {"business_id": "100001-1", "label": "ei_kiinnosta"},
            {"business_id": "100002-1", "label": "hakenee"},  # hakenee -> positiivinen
        ]
    )


def test_build_features_maps_labels_to_targets():
    mod = _load()
    merged = mod.build_features(_companies(), _labels())
    assert list(merged["target"]) == [1, 0, 1]  # kiinnostaa, ei_kiinnosta, hakenee


def test_build_features_joins_on_business_id_as_string():
    mod = _load()
    companies = _companies()
    companies["business_id"] = companies["business_id"].astype(str)
    merged = mod.build_features(companies, _labels())
    assert len(merged) == 3  # vain merkityt rivit
    assert merged["name"].notna().all()


def test_build_features_categoricals_are_category_dtype():
    mod = _load()
    merged = mod.build_features(_companies(), _labels())
    for col in mod.CATEGORICAL_FEATURES:
        assert str(merged[col].dtype) == "category"


def test_build_features_preserves_nan():
    mod = _load()
    companies = _companies()
    companies.loc[0, "llm_is_hiring"] = None
    companies.loc[1, "website_status"] = None  # -> "puuttuu" kategoria
    merged = mod.build_features(companies, _labels())
    assert pd.isna(merged["llm_is_hiring"]).iloc[0]
    assert merged["website_status"].iloc[1] == "puuttuu"


def test_prepare_features_adds_missing_columns_as_nan():
    mod = _load()
    bare = pd.DataFrame({"business_id": ["1-1"], "name": ["X"]})
    out = mod.prepare_features(bare)
    for col in mod.NUMERIC_FEATURES:
        assert col in out.columns
    assert pd.isna(out["llm_is_hiring"]).iloc[0]
    assert int(out["has_website"].iloc[0]) == 0  # ei website-sarakettakaan


def test_prepare_features_converts_registered_to_year():
    mod = _load()
    out = mod.prepare_features(_companies())
    assert out["registered"].tolist() == [2015, 2015, 2015, 2015]


def test_cold_start_counts():
    mod = _load()
    total, pos, neg = mod.cold_start_counts(_labels())
    assert (total, pos, neg) == (3, 2, 1)


def test_cold_start_gate_constants():
    mod = _load()
    assert mod.MIN_ROWS == 30 and mod.MIN_POS == 8 and mod.MIN_NEG == 8


def test_auc_score_perfect_and_ties():
    mod = _load()
    assert mod._auc_score([1, 0, 1, 0], [0.9, 0.1, 0.8, 0.2]) == 1.0
    assert mod._auc_score([1, 0], [0.5, 0.5]) == 0.5  # tasapeli -> 0.5
    assert pd.isna(mod._auc_score([1, 1], [0.9, 0.8]))  # vain yksi luokka