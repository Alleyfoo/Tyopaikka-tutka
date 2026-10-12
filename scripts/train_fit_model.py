#!/usr/bin/env python
"""Train an employer-fit model (LightGBM) from dashboard feedback.

The Työnantajasopivuus tab records per-company curation clicks
(kiinnostaa / ei_kiinnosta / hakenee) in data/feedback.sqlite. This script
turns those labels into a probability (`fit_prob`, 0-1) that a company is a
good employer for the user, and writes it additively into companies.csv.

Cold-start gate
---------------
Few labels would only teach the model noise, so nothing happens below
MIN_ROWS=30 total, MIN_POS=8 positives, MIN_NEG=8 negatives: the script prints
a Finnish fallback message and exits 0 without touching anything. Until the
gate opens, the rule-based `e_fit` score stays the ranking signal.

Usage
-----
  python scripts/train_fit_model.py            # train (or fall back cleanly)
  python scripts/train_fit_model.py --dry-run  # train, don't touch the CSV
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from apprscan import feedback
from apprscan.employer_fit import add_employer_scores

_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = _ROOT / "out" / "companies.csv"
FEEDBACK_DB = _ROOT / "data" / "feedback.sqlite"
MODEL_PATH = _ROOT / "data" / "fit_model.txt"
META_PATH = _ROOT / "data" / "fit_model_meta.json"

# Below any of these the model would only learn noise — fall back to e_fit.
MIN_ROWS = 30
MIN_POS = 8
MIN_NEG = 8

# kiinnostaa/hakenee -> positive (1), ei_kiinnosta -> negative (0)
LABEL_TO_TARGET = {"kiinnostaa": 1, "hakenee": 1, "ei_kiinnosta": 0}

CATEGORICAL_FEATURES = ["industry", "website_status"]
NUMERIC_FEATURES = [
    "e_industry", "e_hiring", "e_commute", "dist_km", "registered",
    "llm_is_hiring", "llm_has_shop", "llm_has_online_sales", "has_website",
]
FEATURE_COLS = NUMERIC_FEATURES + CATEGORICAL_FEATURES

FALLBACK_MESSAGE = (
    "Merkkaa lisää yrityksiä dashboardissa — sääntöpisteet e_fit toimivat siihen asti. "
    f"(Tarvitaan vähintään {MIN_ROWS} merkintää, joista {MIN_POS} positiivista ja "
    f"{MIN_NEG} negatiivista.)"
)


def prepare_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Make any companies frame feature-ready. Pure: no IO, no side effects.

    Adds employer scores if missing, normalises column names, extracts the
    registration year, marks categoricals and fills absent numeric columns
    with NaN (LightGBM handles missing natively).
    """
    out = frame.copy()
    if "e_fit" not in out.columns:
        if "toi_code" in out.columns:
            out = add_employer_scores(out)
        else:
            # No TOI codes -> no industry/commute scoring possible; NaN features.
            for col in ("e_industry", "e_hiring", "e_commute", "e_fit"):
                out[col] = float("nan")
    # dist_km is the feature name; CSV may carry distance_km.
    if "dist_km" not in out.columns and "distance_km" in out.columns:
        out["dist_km"] = out["distance_km"]
    # has_website: any website column with a value.
    site = out["best_website"] if "best_website" in out.columns else out.get("website")
    out["has_website"] = site.notna().astype(int) if site is not None else 0
    # registered is a date string like '2015-02-01' — keep just the year.
    if "registered" in out.columns:
        out["registered"] = pd.to_numeric(
            out["registered"].astype(str).str[:4], errors="coerce"
        )
    for col in CATEGORICAL_FEATURES:
        if col in out.columns:
            out[col] = out[col].fillna("puuttuu").astype(str).astype("category")
        else:
            out[col] = pd.Series(["puuttuu"] * len(out), index=out.index, dtype="category")
    for col in NUMERIC_FEATURES:
        if col not in out.columns:
            out[col] = float("nan")
    return out


def build_features(
    companies: pd.DataFrame, labels: pd.DataFrame
) -> pd.DataFrame:
    """Join companies with feedback labels into a training frame.

    One row per labelled company: prepared features + 0/1 ``target``.
    Companies without a label are left out.
    """
    labs = labels.copy()
    labs["_bid"] = labs["business_id"].astype(str)
    frame = companies.copy()
    frame["_bid"] = frame["business_id"].astype(str)
    merged = labs[["_bid", "label"]].merge(
        prepare_features(frame), on="_bid", how="left"
    )
    merged["target"] = merged["label"].map(LABEL_TO_TARGET)
    return merged


def cold_start_counts(labels: pd.DataFrame) -> tuple[int, int, int]:
    """(total, positives, negatives) from a feedback labels frame."""
    targets = labels["label"].map(LABEL_TO_TARGET).dropna().astype(int)
    return int(len(targets)), int((targets == 1).sum()), int((targets == 0).sum())


def _auc_score(y_true, y_pred) -> float:
    """AUC without sklearn: rank-based (Mann-Whitney U / (n_pos * n_neg))."""
    y_true = pd.Series(y_true).astype(int).to_numpy()
    y_pred = pd.Series(y_pred).to_numpy()
    pos = y_pred[y_true == 1]
    neg = y_pred[y_true == 0]
    if len(pos) == 0 or len(neg) == 0:
        return float("nan")
    wins = sum((p > n) for p in pos for n in neg) + 0.5 * sum(
        (p == n) for p in pos for n in neg
    )
    return float(wins / (len(pos) * len(neg)))


def _train(train_frame: pd.DataFrame) -> tuple["object", float | None]:
    """Train LightGBM on the feature frame. Returns (booster, holdout_auc).

    With >= 100 rows a 20% stratified-by-shuffle holdout gives an honest AUC;
    below that every label is too precious to hold out.
    """
    import lightgbm as lgb  # lazy: build_features tests need no lightgbm

    params = {
        "objective": "binary",
        "metric": "auc",
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_data_in_leaf": 5,  # tiny data — keep leaves from memorising
        "seed": 42,
        "verbose": -1,
    }
    if len(train_frame) >= 100:
        shuffled = train_frame.sample(frac=1.0, random_state=42)
        cut = max(1, int(len(shuffled) * 0.8))
        train_part = shuffled.iloc[:cut]
        valid_part = shuffled.iloc[cut:]
        booster = lgb.train(
            params,
            lgb.Dataset(
                train_part[FEATURE_COLS], train_part["target"],
                categorical_feature=CATEGORICAL_FEATURES,
            ),
            num_boost_round=200,
            valid_sets=[lgb.Dataset(
                valid_part[FEATURE_COLS], valid_part["target"],
                categorical_feature=CATEGORICAL_FEATURES,
            )],
            callbacks=[lgb.early_stopping(20, verbose=False)],
        )
        auc = _auc_score(valid_part["target"], booster.predict(valid_part[FEATURE_COLS]))
        return booster, auc

    booster = lgb.train(
        params,
        lgb.Dataset(
            train_frame[FEATURE_COLS], train_frame["target"],
            categorical_feature=CATEGORICAL_FEATURES,
        ),
        num_boost_round=200,
    )
    return booster, None


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", default=str(CSV_PATH))
    ap.add_argument("--feedback-db", default=str(FEEDBACK_DB))
    ap.add_argument("--model-out", default=str(MODEL_PATH))
    ap.add_argument("--meta-out", default=str(META_PATH))
    ap.add_argument("--dry-run", action="store_true",
                    help="Train and save the model but don't write fit_prob into the CSV")
    args = ap.parse_args()

    if not Path(args.feedback_db).exists():
        print(FALLBACK_MESSAGE)
        return

    conn = feedback.connect(args.feedback_db)
    labels = feedback.load(conn)
    conn.close()

    total, pos, neg = cold_start_counts(labels)
    print(f"Palaute: {total} merkintää ({pos} positiivista, {neg} negatiivista)")
    if total < MIN_ROWS or pos < MIN_POS or neg < MIN_NEG:
        print(FALLBACK_MESSAGE)
        return

    companies = pd.read_csv(args.csv, encoding="utf-8-sig")
    train_frame = build_features(companies, labels)

    booster, auc = _train(train_frame)

    Path(args.model_out).parent.mkdir(parents=True, exist_ok=True)
    booster.save_model(args.model_out)
    meta = {
        "rows": int(len(train_frame)),
        "positives": pos,
        "negatives": neg,
        "features": FEATURE_COLS,
        "auc_holdout": auc,
        "trained_at": pd.Timestamp.utcnow().isoformat(),
    }
    Path(args.meta_out).write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"Malli tallennettu: {args.model_out}")
    if auc is not None:
        print(f"Holdout AUC: {auc:.3f}")

    if args.dry_run:
        print("Dry run — fit_prob:ia ei kirjoitettu CSV:hen.")
        return

    # Predict fit_prob for ALL rows, write back additively. Categories must
    # match the training categories so the booster sees the same codes.
    full = prepare_features(companies)
    for col in CATEGORICAL_FEATURES:
        full[col] = full[col].astype(
            pd.CategoricalDtype(categories=train_frame[col].cat.categories)
        )
    full["fit_prob"] = booster.predict(full[FEATURE_COLS])
    # Write back the ORIGINAL schema + fit_prob only — prepare_features may
    # have reshaped scratch columns (e.g. registered -> year) that must not
    # replace the CSV's real values.
    out = full[list(companies.columns) + ["fit_prob"]]
    out.to_csv(args.csv, index=False, encoding="utf-8-sig")
    print(f"Kirjoitettiin fit_prob {len(out)} riville: {args.csv}")


if __name__ == "__main__":
    main()