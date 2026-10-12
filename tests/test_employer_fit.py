import pandas as pd

from apprscan.employer_fit import (
    COMMUTE_BANDS_MIN,
    EMPLOYER_SCORE_MAX,
    add_employer_scores,
    commute_score,
    hiring_score,
    industry_score,
)


def test_industry_score_prefixes():
    assert industry_score(6201) == 4        # ohjelmistot
    assert industry_score("6202.0") == 4    # float-formatted TOI from the CSV
    assert industry_score("6311") == 4      # IT-palvelut (saman perheen arvo)
    assert industry_score("781") == 3       # henkilöstöpalvelut
    assert industry_score("64") == 2        # rahoitus
    assert industry_score("73") == 1        # markkinointi
    assert industry_score("4711") == 0      # vähittäiskauppa -> ei pisteitä
    assert industry_score(None) == 0


def test_hiring_score_layers():
    assert hiring_score(True, "live") == 3
    assert hiring_score("True", "live") == 3
    assert hiring_score("true", "") == 3
    assert hiring_score(False, "live") == 1   # sivut elossa, rekry havaitsematon
    assert hiring_score(None, "live") == 1
    assert hiring_score("False", "") == 0
    assert hiring_score(None, "") == 0
    assert hiring_score(True, "") == 3


def test_commute_score_bands():
    assert commute_score(30) == 3
    assert commute_score(COMMUTE_BANDS_MIN[0]) == 3
    assert commute_score(40) == 2
    assert commute_score(COMMUTE_BANDS_MIN[1]) == 2
    assert commute_score(60) == 1
    assert commute_score(COMMUTE_BANDS_MIN[2]) == 1
    assert commute_score(80) == 0
    assert commute_score(None) == 0
    assert commute_score(float("nan")) == 0


def _frame(**overrides):
    row = {
        "business_id": "1",
        "toi_code": "6201",
        "llm_is_hiring": None,
        "website_status": "live",
        "nearest_station": "Kerava",
        "distance_km": 1.0,
        "commute_min": 31.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_add_employer_scores_with_existing_commute_column():
    out = add_employer_scores(_frame())
    assert out["e_industry"].iloc[0] == 4
    assert out["e_hiring"].iloc[0] == 1
    assert out["e_commute"].iloc[0] == 3
    assert out["e_fit"].iloc[0] == 8
    assert out["e_fit"].iloc[0] <= EMPLOYER_SCORE_MAX


def test_add_employer_scores_ignores_commute_args_when_column_present():
    # commute_min-sarake on jo laskettu -> origin/mode-argumentit eivät vaikuta
    out = add_employer_scores(_frame(commute_min=80.0), origin="Lahti", mode="walk")
    assert out["e_commute"].iloc[0] == 0


def test_add_employer_scores_computes_commute_when_missing():
    frame = _frame()
    frame = frame.drop(columns=["commute_min"])
    out = add_employer_scores(frame, mode="bus", overhead_min=10.0)
    # Kerava on lähin asema -> paras commute-piste
    assert out["e_commute"].iloc[0] == 3
    # Unknown station -> NaN minutes -> 0 points, ei poikkeusta
    out = add_employer_scores(_frame(nearest_station="Not a station", commute_min=None).drop(
        columns=["commute_min"]
    ))
    assert out["e_commute"].iloc[0] == 0


def test_add_employer_scores_tolerates_missing_optional_columns():
    frame = pd.DataFrame(
        [{"business_id": "1", "toi_code": "6201", "nearest_station": "Kerava", "distance_km": 1.0}]
    )
    out = add_employer_scores(frame, mode="bus", overhead_min=10.0)
    assert out["e_hiring"].iloc[0] == 0  # ei llm_is_hiring eikä website_status
    assert out["e_commute"].iloc[0] == 3
    assert out["e_fit"].iloc[0] == 7