"""Fit the v2 serving model, log it to MLflow, and score the live 2026 population.

Everything the experiments produced so far is a MODEL DEFINITION: a manifest that says which
columns went in and which were held out, plus out-of-fold metrics. Nothing was a loadable
estimator. This script closes that gap, so a scored row resolves to an artifact rather than a
recipe (DATA-2288's registration item, pulled forward because its third trigger fired).

What it does, in order:
  1. Fits the serving spec on the FULL working set with recency weights. The ledgered metrics
     come from cross-fitting and forward evaluation; this fit is the one that scores, and it
     sees every labelled row, so its own training numbers are meaningless and are not reported.
  2. Writes a manifest whose `model_id` covers the input ARTIFACTS as well as the column lists
     (see FEATURE_ARTIFACTS below), then logs the estimator to MLflow under that id.
  3. Scores the live population and assigns within-cohort quantile bands.

Why bands and not a threshold: the model's ranking transfers across the composition shift, its
absolute values do not. Verified-vs-live lead-time overlap is 19.5% and 60% of live rows sit
beyond a year out, where calibration is worst (DATA-2241). Quantiles within a comparable cohort
are the only shape that survives that.

Run:
  cd analytics && uv run --with scikit-learn --with pyarrow --with mlflow-skinny \
      --with skops --with databricks-sql-connector python \
      projects/legit_candidate_predictor/score_population.py

`skops` is not optional: MLflow 3's sklearn flavor serialises through it by default, and
without it `log_model` raises ModuleNotFoundError. That failure used to degrade to a warning
and still write the scores, producing a table whose `mlflow_run_id` was null — traceable to
nothing. The run now aborts instead; pass --allow-unlogged only if you accept that.
"""

import hashlib
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import (  # noqa: E402
    CAT_COLS,
    CLAIM_MASKS,
    COMPOSED_V2,
    FAMILY_SPECS,
    engineer,
    make_hgb_frame,
    recency_weights,
)

HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
)

# `ws_version` fingerprints the working set, but nothing fingerprinted the FEATURE parquets, so
# two runs could share a model_id while a family had been rebuilt underneath them. Rebuilding
# geo on 2026-08-14 proved this is not hypothetical: it changed the composed model by -0.0005
# without touching a single column name. Hashing the artifacts folds them into model_id.
FEATURE_ARTIFACTS = ("geo_features.parquet", *(FAMILY_SPECS[f].file for f in COMPOSED_V2))

SCORING_POP = HERE / "datasets" / "scoring_population.parquet"
OUT = HERE / "datasets" / "scored_population.parquet"

# Evaluation numbers for this DEFINITION, carried onto the run so a scored row resolves to
# honest performance. Measured by exp_family on the frozen folds with the refreshed geo.
EVAL_METRICS = {"oof_roc_auc": 0.8819, "oof_decidable_roc_auc": 0.8362, "forward_roc_auc": 0.8922}


def _oof(x: pd.DataFrame, eng: pd.DataFrame) -> np.ndarray:
    """Out-of-fold predictions on the frozen folds, for building a calibration curve."""
    folds = harness.load_folds()
    fold = eng[["campaign_id"]].merge(folds[["campaign_id", "fold"]], on="campaign_id", how="left")["fold"]
    assert fold.notna().all(), "a row is missing from the frozen folds"
    y = eng["corroborated"].astype(int)
    w = recency_weights(eng)
    out = np.zeros(len(x))
    for k in sorted(fold.unique()):
        tr, va = fold.ne(k).to_numpy(), fold.eq(k).to_numpy()
        m = HistGradientBoostingClassifier(**HGB).fit(x[tr], y[tr], sample_weight=w[tr])
        out[va] = m.predict_proba(x[va])[:, 1]
    return out


def artifact_shas() -> dict[str, str]:
    return {
        name: hashlib.sha256((HERE / "datasets" / name).read_bytes()).hexdigest()[:12]
        for name in FEATURE_ARTIFACTS
    }


def attach_families(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Merge the composed families onto a frame, honouring the registry's exclusions."""
    cols: list[str] = []
    for name in COMPOSED_V2:
        spec = FAMILY_SPECS[name]
        fam = pd.read_parquet(HERE / "datasets" / spec.file)
        keep = [c for c in fam.columns if c != spec.key and c not in spec.exclude]
        before = len(df)
        df = df.merge(fam[[spec.key, *keep]], on=spec.key, how="left", validate="many_to_one")
        assert len(df) == before, f"{name} merge fanned out"
        cols += keep
    return df, cols


def build_frame(
    df: pd.DataFrame, cols: list[str], mask: tuple[str, ...] = ()
) -> tuple[pd.DataFrame, pd.DataFrame]:
    eng = engineer(df)
    x = make_hgb_frame(eng)
    for c in cols:
        s = df[c]
        if s.dtype == object or isinstance(s.dtype, pd.CategoricalDtype):
            x[c] = pd.Categorical(s.astype("object").where(s.notna(), "missing"))
        else:
            x[c] = pd.to_numeric(s, errors="coerce").to_numpy()
    if mask:
        absent = [c for c in mask if c not in x.columns]
        assert not absent, f"mask names columns not in the frame: {absent}"
        x = x.drop(columns=list(mask))
    return x, eng


def align_categories(train: pd.DataFrame, score: pd.DataFrame) -> pd.DataFrame:
    """Force the scoring frame's categoricals onto the training vocabulary.

    A category the model never saw must become an unseen level, not a silently different
    encoding. HGB maps unknown categories to the missing bin, which is the behaviour we want.
    """
    for c in train.columns:
        if isinstance(train[c].dtype, pd.CategoricalDtype):
            score[c] = pd.Categorical(score[c].astype("object"), categories=train[c].cat.categories)
    return score[train.columns]


def cohort_bands(
    df: pd.DataFrame, p: np.ndarray, min_cell: int = 100, by_office: bool = True
) -> pd.DataFrame:
    """Within-cohort quantile bands. Cohort = lead-time bucket x office type, with fallback.

    Absolute score values do not transfer in-cycle, so a band is a position among comparable
    rows, never a probability cut. Thin cells fall back to the lead-time bucket alone rather
    than producing quantiles over a handful of rows; `cohort_fallback` records which happened.

    `by_office=False` drops office type from the cohort, and the PROFILE model requires it.
    Ranking inside office-type cohorts forces every office type to hold exactly 20% of every
    band by construction, so a report profiling bands by office type would find a flat
    distribution no matter what the data said. Masking office type out of the features but
    leaving it in the cohort would defeat the whole point of the variant. Lead time stays in
    the cohort either way: it is not one of the claim variables and it dominates the base rate.
    """
    # Named `signup_lead_*` on purpose. This is how far AHEAD OF THEIR ELECTION the person
    # signed up, i.e. how speculative the signup was at the time. It is NOT how far away the
    # election is now — 88% of the live population votes within 90 days regardless of which
    # bucket they sit in. The shorter name `lead_*` caused exactly that misreading once.
    lead = pd.cut(
        df["lead_time_days"],
        bins=[-np.inf, 90, 180, 365, np.inf],
        labels=["signup_lead_0_90d", "signup_lead_91_180d", "signup_lead_181_365d", "signup_lead_365d_plus"],
    )
    if by_office:
        office = df["office_type"].fillna("missing").astype(str)
        fine = lead.astype(str) + " | " + office
        sizes = fine.value_counts()
        fallback = fine.map(sizes).lt(min_cell)
        key = fine.where(~fallback, lead.astype(str) + " | (all offices)")
    else:
        key = lead.astype(str) + " | (all offices)"
        fallback = pd.Series(False, index=df.index)

    out = pd.DataFrame({"cohort_key": key.to_numpy(), "cohort_fallback": fallback.to_numpy()}, index=df.index)
    out["cohort_n"] = out.groupby("cohort_key")["cohort_key"].transform("size")
    s = pd.Series(p, index=df.index)
    out["score_pct_within_cohort"] = s.groupby(out["cohort_key"]).rank(pct=True).mul(100).round(2)
    # Quintiles within cohort. Q5 = highest scoring fifth of comparable rows. Deliberately
    # neutral names: the bottom band is "lowest ranked", never "not a candidate" (2026-08-13
    # error-cost decision — the bottom decile is still 44-52% real).
    out["band"] = pd.cut(
        out["score_pct_within_cohort"],
        bins=[0, 20, 40, 60, 80, 100],
        labels=["Q1", "Q2", "Q3", "Q4", "Q5"],
        include_lowest=True,
    ).astype(str)
    return out


TIER_LIKELY, TIER_UNLIKELY = 0.60, 0.10
BUCKET_EDGES = [-0.001, 0.05, 0.15, 0.35, 0.60, 1.01]
BUCKET_LABELS = ["p_under_5", "p_5_15", "p_15_35", "p_35_60", "p_60_plus"]
OOF_FILE = HERE / "datasets" / "oof_br_position_gp_user_details_incumbency.parquet"
# The combined claim mask the sales/product report needs. One variant, four profilable axes.
PROFILE_CLAIM = "sales_profile"


def calibrate(
    live: pd.DataFrame, live_pct: pd.Series, *, mask: tuple[str, ...] = (), by_office: bool = True
) -> tuple[np.ndarray, pd.Series, pd.Series]:
    """Turn a within-cohort rank into an expected corroboration RATE, then into a tier.

    A percentile answers "where does this person sit among peers", which is not a number you
    can put on a slide. This maps it onto what actually happened to labelled rows at the same
    rank in the same lead-time bucket, so the output means "roughly this share of people like
    this end up corroborated".

    Isotonic rather than the raw decile table: it is monotone by construction (a higher rank
    can never map to a lower rate, which the thin long-lead cells violate by noise alone) and
    it does not need every cell to be large.

    The load-bearing assumption: labelled rows in a lead bucket stand in for live rows in the
    same bucket. That is strongest for short lead times and weakest for 365d+, where only 454
    labelled rows exist against 6,353 live ones. Treat long-lead rates as directional.
    """
    from sklearn.isotonic import IsotonicRegression

    ws = pd.read_parquet(harness.WORKING_SET)
    if mask:
        # A masked model needs its OWN labelled reference curve: the serving model's OOF
        # scores encode the very columns this variant holds out, so calibrating against them
        # would smuggle the claim variables back in through the calibration.
        ws_f, cols_f = attach_families(ws)
        x, eng = build_frame(ws_f, cols_f, mask)
        lab = ws_f.assign(oof_hgb=_oof(x, eng), corroborated=eng["corroborated"].astype(int))
    else:
        oof = pd.read_parquet(OOF_FILE)
        lab = ws.merge(
            oof[["campaign_id", "corroborated", "oof_hgb"]],
            on="campaign_id",
            how="inner",
            validate="one_to_one",
            suffixes=("_ws", ""),
        )
    lab["lead_time_days"] = (
        pd.to_datetime(lab["general_election_date"]) - pd.to_datetime(lab["signup_date"])
    ).dt.days
    lab_b = cohort_bands(lab, lab["oof_hgb"].to_numpy(), by_office=by_office)
    lab["bucket"] = lab_b["cohort_key"].str.split(" | ", regex=False).str[0]
    lab["pct"] = lab_b["score_pct_within_cohort"]

    live_bucket = live["cohort_key"].str.split(" | ", regex=False).str[0]
    out = np.full(len(live), np.nan)
    print("\ncalibration curve per lead bucket (labelled rows -> expected corroboration):")
    for bucket, g in lab.groupby("bucket"):
        iso = IsotonicRegression(y_min=0, y_max=1, out_of_bounds="clip").fit(
            g["pct"].to_numpy(), g["corroborated"].to_numpy()
        )
        sel = live_bucket.eq(bucket).to_numpy()
        out[sel] = iso.predict(live_pct[sel].to_numpy())
        print(
            f"  {bucket:<16} labelled n={len(g):>6,}  base {g.corroborated.mean():.3f}  "
            f"live n={sel.sum():>6,}  "
            f"maps pct 10/50/90 -> {iso.predict([10, 50, 90]).round(3)}"
        )

    tier = pd.Series(
        np.where(
            out >= TIER_LIKELY,
            "likely_corroborated",
            np.where(out < TIER_UNLIKELY, "unlikely_corroborated", "unsure"),
        ),
        index=live.index,
    )
    # Five absolute probability bands. The three-way tier collapses a lot of real signal:
    # 5-15% and 15-35% behave very differently (0.9% vs 2.0% already corroborated) and the
    # coarse cut called both "unsure". Bands are on the calibrated rate, so they mean the
    # same thing in every cohort and the table can be sliced by anything. Boundaries are a
    # judgement call, not a property of the model — change them here.
    bucket = pd.cut(out, BUCKET_EDGES, labels=BUCKET_LABELS).astype(str)
    return out, tier, pd.Series(bucket, index=live.index)


def main(allow_unlogged: bool = False) -> None:
    ws = pd.read_parquet(harness.WORKING_SET)
    ws, cols = attach_families(ws)
    x_train, eng_train = build_frame(ws, cols)
    y = eng_train["corroborated"].astype(int)
    w = recency_weights(eng_train)
    print(f"training rows {len(x_train):,}  features {x_train.shape[1]}  " f"positive {y.mean():.3f}")

    model = HistGradientBoostingClassifier(**HGB).fit(x_train, y, sample_weight=w)

    shas = artifact_shas()
    man = harness.model_manifest(
        "v2 serving FIT (full working set) — scores win_candidate_scores_20260814",
        features={
            "cat": [c for c in CAT_COLS if c in x_train.columns],
            "num": [c for c in x_train.columns if c not in CAT_COLS and c not in cols],
            "family": cols,
        },
        excluded={
            c: f"{name}: {why}" for name in COMPOSED_V2 for c, why in FAMILY_SPECS[name].exclude.items()
        },
        hyperparams=HGB
        | {
            "recency_half_life_days": 240,
            "tier": "serving",
            "families": list(COMPOSED_V2),
            "fit_scope": "full_working_set",
            "feature_artifact_sha": shas,
        },
        notes="Fitted estimator behind the scored live population. Metrics on this run are the "
        "frozen-fold evaluation of the same definition, NOT of this fit (which saw every "
        "labelled row).",
    )
    model_id = man["model_id"]
    print(f"\nmodel_id {model_id}   artifacts {shas}")

    run_id = log_model(model, x_train, model_id, shas, allow_unlogged)

    # --- score the live population -------------------------------------------------------
    sp = pd.read_parquet(SCORING_POP)
    sp, score_cols = attach_families(sp)
    assert score_cols == cols, "family columns differ between training and scoring"
    x_score, _ = build_frame(sp, cols)
    x_score = align_categories(x_train, x_score)
    assert list(x_score.columns) == list(x_train.columns), "feature order differs"

    p = model.predict_proba(x_score)[:, 1]
    print(
        f"\nscored {len(p):,} rows   mean p {p.mean():.3f}  "
        f"median {np.median(p):.3f}  p10 {np.quantile(p, .1):.3f}  "
        f"p90 {np.quantile(p, .9):.3f}"
    )

    bands = cohort_bands(sp, p)
    expected, tier, bucket = calibrate(bands, bands["score_pct_within_cohort"])
    out = pd.DataFrame(
        {
            "user_id": sp["user_id"],
            "campaign_id": sp["campaign_id"],
            "p_corroborated": p.round(6),
            "expected_corroboration_rate": np.round(expected, 4),
            "tier": tier.to_numpy(),
            "score_bucket": bucket.to_numpy(),
            "score_pct_within_cohort": bands["score_pct_within_cohort"],
            "band": bands["band"],
            "cohort_key": bands["cohort_key"],
            "cohort_n": bands["cohort_n"],
            "cohort_fallback": bands["cohort_fallback"],
            "lead_time_days": sp["lead_time_days"],
            "days_to_election": sp["days_to_election"],
            "general_election_date": pd.to_datetime(sp["general_election_date"]).dt.date,
            "deadline_passed": sp["deadline_passed"],
            "model_id": model_id,
            "mlflow_run_id": run_id,
            "ws_version": harness.ws_version(),
            "scored_at": pd.Timestamp.utcnow().tz_localize(None).normalize().date(),
        }
    )
    # --- second model: the PROFILE variant ------------------------------------------------
    # Same rows, same pipeline, but blind to state, office, incumbency and district size, so
    # a report can profile the scored groups ON those variables without restating the rule
    # that defined the groups. Its cohort drops office type for the same reason (see
    # cohort_bands): ranking within office would flatten the very distribution being reported.
    mask = CLAIM_MASKS[PROFILE_CLAIM]
    x_train_p, _ = build_frame(ws, cols, mask)
    model_p = HistGradientBoostingClassifier(**HGB).fit(x_train_p, y, sample_weight=w)
    man_p = harness.model_manifest(
        f"v2 PROFILE FIT ({PROFILE_CLAIM}) — profile columns of win_candidate_scores_20260814",
        features={
            "cat": [c for c in CAT_COLS if c in x_train_p.columns],
            "num": [c for c in x_train_p.columns if c not in CAT_COLS and c not in cols],
            "family": [c for c in cols if c not in mask],
        },
        excluded={
            c: f"{name}: {why}" for name in COMPOSED_V2 for c, why in FAMILY_SPECS[name].exclude.items()
        }
        | {
            c: f"analysis tier: held out so the report can profile {PROFILE_CLAIM} "
            f"variables without restating the scoring rule"
            for c in mask
        },
        hyperparams=HGB
        | {
            "recency_half_life_days": 240,
            "tier": "analysis",
            "claim": PROFILE_CLAIM,
            "families": list(COMPOSED_V2),
            "fit_scope": "full_working_set",
            "cohort_includes_office": False,
            "feature_artifact_sha": shas,
        },
        notes="Profile variant. Use for any claim about state, office type, incumbency or "
        "district size. Do NOT use it to prioritise outreach; the serving model is "
        "strictly better informed.",
    )
    model_id_p = man_p["model_id"]
    print(f"\nprofile model_id {model_id_p}   {x_train_p.shape[1]} features " f"({len(mask)} masked)")
    run_id_p = log_model(model_p, x_train_p, model_id_p, shas, allow_unlogged)

    x_score_p, _ = build_frame(sp, cols, mask)
    x_score_p = align_categories(x_train_p, x_score_p)
    p_p = model_p.predict_proba(x_score_p)[:, 1]
    bands_p = cohort_bands(sp, p_p, by_office=False)
    expected_p, _tier_p, bucket_p = calibrate(
        bands_p, bands_p["score_pct_within_cohort"], mask=mask, by_office=False
    )

    out["profile_p_corroborated"] = p_p.round(6)
    out["profile_expected_corroboration_rate"] = np.round(expected_p, 4)
    out["profile_score_bucket"] = bucket_p.to_numpy()
    out["profile_score_pct_within_cohort"] = bands_p["score_pct_within_cohort"].to_numpy()
    out["profile_cohort_key"] = bands_p["cohort_key"].to_numpy()
    out["profile_model_id"] = model_id_p
    out["profile_mlflow_run_id"] = run_id_p

    print("\n=== PROFILE buckets (use these for state / office / incumbency / size cuts) ===")
    pb = (
        out.groupby("profile_score_bucket")
        .agg(n=("profile_score_bucket", "size"), expected=("profile_expected_corroboration_rate", "mean"))
        .round(3)
    )
    pb["share"] = (pb["n"] / len(out)).round(3)
    print(pb.to_string())
    print("\nagreement with the serving bucket (rows in the same band):")
    print(f"  {(out['score_bucket'] == out['profile_score_bucket']).mean():.1%}")

    out.to_parquet(OUT, index=False)
    print(f"\nsaved -> {OUT}")

    print("\ncohort sizes (fallback marked *):")
    ck = bands.groupby("cohort_key").agg(n=("cohort_n", "first"), fb=("cohort_fallback", "first"))
    for k, r in ck.sort_values("n", ascending=False).iterrows():
        print(f"  {'*' if r.fb else ' '} {k:<38} {r.n:>6,}")

    print("\n=== TIER MIX (the cut to present) ===")
    tm = (
        out.groupby("tier")
        .agg(n=("tier", "size"), mean_expected=("expected_corroboration_rate", "mean"))
        .round(3)
    )
    tm["share"] = (tm["n"] / len(out)).round(3)
    print(tm.to_string())
    print("\ntier by lead bucket:")
    lb = out["cohort_key"].str.split(" | ", regex=False).str[0]
    print(pd.crosstab(lb, out["tier"]).to_string())

    print("\nmean p by band (sanity: must increase Q1 -> Q5):")
    print(pd.Series(p, index=out.index).groupby(out["band"]).agg(["size", "mean"]).round(4).to_string())

    # Censoring check: the by-deadline arrivals count is final only where the deadline passed.
    print("\nscore by deadline status (train/serve skew watch):")
    print(
        pd.Series(p, index=out.index)
        .groupby(out["deadline_passed"])
        .agg(["size", "mean"])
        .round(4)
        .to_string()
    )


def log_model(
    model, x_train: pd.DataFrame, model_id: str, shas: dict[str, str], allow_unlogged: bool = False
) -> str | None:
    """Log the fitted estimator to MLflow under this model_id.

    Raises on failure by default. The point of this run is that a scored row resolves to a
    loadable artifact; degrading quietly would leave the table pointing at nothing while
    looking complete.
    """
    try:
        import mlflow
        from mlflow.models import infer_signature

        mlflow.set_tracking_uri(harness.MLFLOW_TRACKING_URI)
        mlflow.set_experiment(harness.MLFLOW_EXPERIMENT)
        with mlflow.start_run(run_name=f"serving fit {model_id}") as run:
            mlflow.log_params(
                {
                    "model_id": model_id,
                    "tier": "serving",
                    "fit_scope": "full_working_set",
                    "ws_version": harness.ws_version(),
                    "families": ",".join(COMPOSED_V2),
                    **shas,
                }
            )
            mlflow.log_metrics(EVAL_METRICS)
            mlflow.set_tags(
                {
                    "area": "serving",
                    "notes": "Fitted estimator behind win_candidate_scores_20260814. Metrics are "
                    "the frozen-fold evaluation of this definition, not of this fit.",
                    "score_semantics": "ranks CORROBORATION, not realness; absolute values are "
                    "biased high in-cycle; use within-cohort bands",
                }
            )
            # cloudpickle, not the MLflow 3 default. skops refuses to round-trip
            # HistGradientBoostingClassifier: it carries `functools.partial` and
            # `sklearn.utils.validation.check_array`, neither of which is in skops' trusted
            # type list, so the post-save load check raises UntrustedTypesFoundException.
            mlflow.sklearn.log_model(
                model,
                name="model",
                serialization_format="cloudpickle",
                signature=infer_signature(x_train.head(200), model.predict_proba(x_train.head(200))[:, 1]),
            )
            manifest = harness.MANIFESTS / f"{model_id}.json"
            if manifest.exists():
                mlflow.log_artifact(str(manifest))
            print(f"mlflow run {run.info.run_id} (estimator logged)")
            return run.info.run_id
    except Exception as e:
        if not allow_unlogged:
            raise RuntimeError(
                f"mlflow model logging failed ({type(e).__name__}: {e}). The scored table "
                f"would not resolve to a fitted artifact. Fix the cause, or re-run with "
                f"--allow-unlogged if you accept an untraceable table."
            ) from e
        print(
            f"WARNING: mlflow model logging failed ({type(e).__name__}: {e}); "
            f"scores still produced but NOT traceable to an artifact"
        )
        return None


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--allow-unlogged",
        action="store_true",
        help="write scores even if the estimator could not be logged to MLflow",
    )
    main(ap.parse_args().allow_unlogged)
