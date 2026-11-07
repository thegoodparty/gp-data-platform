"""DATA-2241 Area 4, item 4: per-cohort calibration verification.

Carried from the v1 parking lot. The question is not "is the score calibrated" but "does a band
mean the same thing in every cohort", because DATA-2247 bands WITHIN cohort and a band is only
interpretable if the score's relationship to reality is stable across the cohorts being compared.

Two targets, deliberately. The model was trained on `corroborated`, so that is what it can be
calibrated against in the ordinary sense. But DATA-2238 established that consumers read the score
as realness, and the two diverge hard at the bottom: the lowest decile is 0.5% corroborated and
44-52% real. Verifying only against corroboration would reproduce exactly the error that work
exposed, so every cohort is scored against both. Realness comes from the PU weights
(`pu_weights.parquet`): a row's expected realness is 1 if corroborated, else its design-weighted
p(real | label=0).

THE LIMIT, which is the headline rather than a footnote. Every cohort here is mature by
construction, because the working set only contains completed elections. The live in-cycle
population cannot be calibration-verified at all: at 61-90 days from its election only 8.5-11% of
eventual corroborations have arrived (see `spike_label_maturity.py`). So this script verifies
calibration where labels exist and then measures how far the live population sits OUTSIDE that
range. If the in-cycle lead-time distribution falls outside the verified span, absolute band values
do not transfer and within-cohort quantile bands are the only defensible shape.

Model vintage is a parameter, not a constant: DATA-2314 will produce a composed v2 model and this
needs re-running against it.

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/calibration_cohorts.py --ledger
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402

DATASETS = HERE / "datasets"
WS_FILE = DATASETS / "working_set_v2.parquet"
WS_VERSION = "b04cdd5ac4b1"
DEFAULT_OOF = "oof_ws_rebuild_canonical.parquet"
DEFAULT_MODEL_ID = "c1f1d210c9a5"
PU_WEIGHTS = DATASETS / "pu_weights.parquet"
MATURITY_PROBE = DATASETS / "label_maturity_probe.parquet"

MIN_COHORT_N = 200  # below this, calibration error is dominated by sampling noise
MAX_BINS = 10


def cox_calibration(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    """Calibration slope and intercept: regress the outcome on the predicted log-odds.

    Slope 1 with intercept 0 is perfect. Slope < 1 means the scores are too extreme (over-confident
    at both ends); intercept away from 0 means a systematic level shift. Reported instead of ECE
    alone because ECE hides WHICH way a cohort is wrong, and the direction is what a band designer
    needs.
    """
    from sklearn.linear_model import LogisticRegression

    if len(np.unique(y)) < 2:
        return (np.nan, np.nan)
    logit = np.log(np.clip(p, 1e-6, 1 - 1e-6) / (1 - np.clip(p, 1e-6, 1 - 1e-6)))
    # C=inf rather than penalty=None: the latter is deprecated in sklearn 1.8 and removed in 1.10.
    # Unregularised is required here, since any shrinkage biases the slope toward zero and would
    # manufacture the very miscalibration this function measures.
    m = LogisticRegression(C=np.inf, solver="lbfgs", max_iter=1000)
    m.fit(logit.reshape(-1, 1), y)
    return (float(m.coef_[0][0]), float(m.intercept_[0]))


def ece(p: np.ndarray, actual: np.ndarray, bins: int) -> float:
    """Expected calibration error against `actual`, which may be a rate rather than a 0/1 label."""
    if bins < 2:
        return np.nan
    q = pd.qcut(p, bins, duplicates="drop")
    df = pd.DataFrame({"q": q, "p": p, "a": actual})
    g = df.groupby("q", observed=True).agg(n=("p", "size"), p=("p", "mean"), a=("a", "mean"))
    return float((g["n"] * (g["p"] - g["a"]).abs()).sum() / len(df))


def cohort_table(df: pd.DataFrame, axis: str) -> pd.DataFrame:
    rows = []
    for name, g in df.groupby(axis, observed=True):
        if len(g) < MIN_COHORT_N:
            rows.append({"axis": axis, "cohort": str(name), "n": len(g), "note": "suppressed, below minimum"})
            continue
        bins = min(MAX_BINS, max(2, len(g) // 50))
        y = g["corroborated"].to_numpy()
        p = g["oof_hgb"].to_numpy()
        slope, intercept = cox_calibration(y, p)
        rows.append(
            {
                "axis": axis,
                "cohort": str(name),
                "n": len(g),
                "base_rate": y.mean(),
                "mean_pred": p.mean(),
                "expected_real": g["expected_real"].mean(),
                "ece_corrob": ece(p, y, bins),
                "ece_real": ece(p, g["expected_real"].to_numpy(), bins),
                "cox_slope": slope,
                "cox_intercept": intercept,
                "note": "",
            }
        )
    return pd.DataFrame(rows)


def lead_bucket(days: pd.Series) -> pd.Series:
    return pd.cut(
        days, [-(10**6), 30, 90, 180, 365, 10**6], labels=["<=30d", "31-90d", "91-180d", "181-365d", ">365d"]
    )


def main(oof_file: str, model_id: str, ledger: bool) -> None:
    assert harness.ws_version(WS_FILE) == WS_VERSION, "not the DATA-2312 canonical working set"
    # Only the cohort columns come from the working set. `corroborated` lives on both frames and
    # taking both would silently suffix them and leave the label ambiguous; the OOF copy is the one
    # the predictions were scored against, so that is the one to keep.
    cohort_cols = [
        "campaign_id",
        "user_id",
        "signup_date",
        "general_election_date",
        "br_normalized_position_type",
        "icp_office_win",
        "campaign_state",
    ]
    ws = pd.read_parquet(WS_FILE)[cohort_cols]
    oof = pd.read_parquet(DATASETS / oof_file)
    pu = pd.read_parquet(PU_WEIGHTS)[["campaign_id", "w_floor"]]

    df = oof.merge(ws, on=["campaign_id", "user_id"], how="left", validate="one_to_one")
    df = df.merge(pu, on="campaign_id", how="left", validate="one_to_one")
    assert len(df) == len(oof)

    # Expected realness: corroborated rows are trustworthy positives; uncorroborated rows carry
    # their design-weighted p(real | label=0). Positives get 1.0, so the weight only touches zeros.
    df["expected_real"] = np.where(df["corroborated"] == 1, 1.0, df["w_floor"].fillna(0.0))

    signup = pd.to_datetime(df["signup_date"])
    election = pd.to_datetime(df["general_election_date"])
    df["lead_days"] = (election - signup).dt.days
    df["lead_bucket"] = lead_bucket(df["lead_days"])
    df["signup_half"] = signup.dt.year.astype(str) + "H" + np.where(signup.dt.month <= 6, "1", "2")
    df["position_type"] = df["br_normalized_position_type"].fillna("missing").astype(str)
    df["icp"] = np.where(
        df["icp_office_win"].isna(),
        "unknown",
        np.where(df["icp_office_win"].astype("boolean").fillna(False), "icp", "non_icp"),
    )
    top_states = df["campaign_state"].value_counts().head(8).index
    df["state_top"] = np.where(df["campaign_state"].isin(top_states), df["campaign_state"], "(other)")

    print(f"model {model_id} on ws {WS_VERSION}: n={len(df):,}")
    pooled_slope, pooled_int = cox_calibration(df["corroborated"].to_numpy(), df["oof_hgb"].to_numpy())
    print(
        f"\npooled: base rate {df.corroborated.mean():.4f}  mean pred "
        f"{df.oof_hgb.mean():.4f}  expected realness {df.expected_real.mean():.4f}"
    )
    print(
        f"pooled ECE vs corroboration {ece(df.oof_hgb.to_numpy(), df.corroborated.to_numpy(), 10):.4f}"
        f"   vs realness {ece(df.oof_hgb.to_numpy(), df.expected_real.to_numpy(), 10):.4f}"
    )
    print(f"pooled Cox slope {pooled_slope:.3f}, intercept {pooled_int:+.3f}")

    pd.set_option("display.width", 200)
    out = []
    for axis in ("lead_bucket", "signup_half", "icp", "position_type", "state_top"):
        t = cohort_table(df, axis)
        out.append(t)
        print(f"\n=== calibration by {axis} ===")
        print(t.round(4).to_string(index=False))
    allt = pd.concat(out, ignore_index=True)

    # Spread across cohorts is the actual deliverable: a band is comparable across cohorts only if
    # the calibration error is stable, not merely small on average.
    ok = allt[allt["note"].eq("") & allt["ece_corrob"].notna()]
    print("\n=== spread of calibration error across cohorts (the load-bearing read) ===")
    for col in ("ece_corrob", "ece_real", "cox_slope"):
        print(
            f"{col:12s} min {ok[col].min():.4f}  median {ok[col].median():.4f}  " f"max {ok[col].max():.4f}"
        )
    worst = ok.nlargest(5, "ece_corrob")[
        ["axis", "cohort", "n", "base_rate", "mean_pred", "ece_corrob", "cox_slope"]
    ]
    print("\nworst-calibrated cohorts vs corroboration:")
    print(worst.round(4).to_string(index=False))

    # --- the extrapolation gap: how far the live population sits outside verified territory ---
    print("\n=== can these results transfer to the live in-cycle population? ===")
    probe = pd.read_parquet(MATURITY_PROBE)
    today = pd.Timestamp.today().normalize()
    for c in ("general_election_date", "signup_date"):
        probe[c] = pd.to_datetime(probe[c])
    inc = probe[probe["general_election_date"] >= today].copy()
    inc["lead_days"] = (inc["general_election_date"] - inc["signup_date"]).dt.days
    inc["lead_bucket"] = lead_bucket(inc["lead_days"])

    comp = (
        pd.DataFrame(
            {
                "verified_share": df["lead_bucket"].value_counts(normalize=True),
                "in_cycle_share": inc["lead_bucket"].value_counts(normalize=True),
            }
        )
        .fillna(0.0)
        .sort_index()
    )
    print("lead-time composition, verified (mature) vs live (in-cycle):")
    print(comp.round(3).to_string())
    print(
        f"\nverified lead-time median {df.lead_days.median():.0f}d, "
        f"live median {inc.lead_days.median():.0f}d"
    )
    overlap = float(np.minimum(comp["verified_share"], comp["in_cycle_share"]).sum())
    print(f"distribution overlap: {overlap:.1%}")

    allt.to_csv(DATASETS / "calibration_cohorts.csv", index=False)
    print(f"\nsaved datasets/calibration_cohorts.csv ({len(allt)} cohort rows)")

    if ledger:
        harness.append_ledger(
            "per-cohort calibration verification (two targets)",
            "oof_cv5",
            "hgb",
            {
                "pooled_ece_corrob": round(ece(df.oof_hgb.to_numpy(), df.corroborated.to_numpy(), 10), 4),
                "pooled_ece_real": round(ece(df.oof_hgb.to_numpy(), df.expected_real.to_numpy(), 10), 4),
                "pooled_cox_slope": round(pooled_slope, 4),
                "pooled_cox_intercept": round(pooled_int, 4),
                "cohort_ece_corrob_max": round(float(ok["ece_corrob"].max()), 4),
                "cohort_ece_real_max": round(float(ok["ece_real"].max()), 4),
                "cohort_cox_slope_min": round(float(ok["cox_slope"].min()), 4),
                "cohort_cox_slope_max": round(float(ok["cox_slope"].max()), 4),
                "n_cohorts_scored": int(len(ok)),
                "lead_time_overlap_verified_vs_live": round(overlap, 4),
            },
            notes="DATA-2241 item 4. Calibrated against BOTH corroboration (the training label) "
            "and expected realness (PU-weighted), because consumers read the score as "
            "realness and the two diverge at the bottom. Cohorts are mature by construction; "
            "the live in-cycle population cannot be calibration-verified at all, so the "
            "lead-time overlap metric is how far any transfer has to reach.",
            area="area4",
            ver=WS_VERSION,
            model_id=model_id,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--oof", default=DEFAULT_OOF, help="OOF parquet under datasets/")
    ap.add_argument("--model-id", default=DEFAULT_MODEL_ID)
    ap.add_argument("--ledger", action="store_true")
    a = ap.parse_args()
    main(a.oof, a.model_id, a.ledger)
