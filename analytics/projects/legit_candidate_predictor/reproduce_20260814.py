"""Reproduce the 2026-08-14 run from the Unity Catalog volume, without a laptop's datasets/.

Two checks, each against a fixed answer:

  scores  Loads both registered champions (serving and profile), scores the 08-14 scoring
          population, rebuilds bands and calibration, and compares every score column with
          model_predictions.win_candidate_scores_20260814. Nothing is fitted: calibration
          reads the cached out-of-fold predictions instead of refitting the fold models.
  aucs    Re-runs the frozen-fold evaluation of the serving definition (and with --profile, the
          profile definition) and checks it against the numbers in SESSION_NOTES.md:
          serving 0.8819 pooled / 0.8362 decidable / 0.8922 forward. This one does fit, because
          an out-of-fold AUC is a property of the fitting procedure, not of one estimator.

Inputs are read from LCP_DATASETS when set. Otherwise they are downloaded once from VOLUME into
a local cache outside the repo, and every file's hash is checked against INPUTS before use, so a
rebuilt or truncated file fails loudly rather than producing a near-miss.

Run (from analytics/):
  uv run --with scikit-learn==1.9.0 --with pandas==2.3.3 --with numpy==2.4.6 --with pyarrow \
      --with mlflow-skinny --with boto3 --with databricks-sdk --with databricks-sql-connector \
      python projects/legit_candidate_predictor/reproduce_20260814.py scores
  ... reproduce_20260814.py aucs [--profile]

The versions are pinned to the logged models' requirements.txt. The estimators are cloudpickled,
and scikit-learn 1.9.0 is what fitted them: a newer release fails inside the categorical encoder.

Auth is whatever `databricks auth login` set up (the SDK default profile, or
DATABRICKS_CONFIG_PROFILE); `scores` also needs DATABRICKS_HTTP_PATH for the comparison query.
On Databricks compute, set LCP_DATASETS to the VOLUME path and skip the download.
"""

import argparse
import hashlib
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
VOLUME = "/Volumes/goodparty_data_catalog/model_predictions/legit_candidate_predictor_inputs/20260814"
CACHE = Path.home() / ".cache" / "legit_candidate_predictor" / "20260814"
SCORES_TABLE = "goodparty_data_catalog.model_predictions.win_candidate_scores_20260814"
SERVING_URI = "models:/goodparty_data_catalog.model_predictions.win_legit_candidate_serving@champion"
PROFILE_URI = "models:/goodparty_data_catalog.model_predictions.win_legit_candidate_profile@champion"

# sha256[:12] of each input as it stood for the 08-14 run. The four feature artifacts match the
# feature_artifact_sha recorded in manifests 21ff4ad08ba3 and caffc2f61d1e.
INPUTS = {
    "working_set.parquet": "b04cdd5ac4b1",
    "folds_b04cdd5ac4b1.parquet": "f353ed4d0721",
    "geo_features.parquet": "5577daf443b9",
    "br_position_features.parquet": "c2a18db96d01",
    "gp_user_details_features.parquet": "d581e6e45dcb",
    "incumbency_features.parquet": "087c32bc4ab9",
    "scoring_population.parquet": "14297d561892",
    "oof_br_position_gp_user_details_incumbency.parquet": "b1f9cd8448bb",
    "oof_br_position_gp_user_details_incumbency_no_sales_profile.parquet": "e3f732cc5eb6",
}
PROFILE_OOF = "oof_br_position_gp_user_details_incumbency_no_sales_profile.parquet"

EXPECTED_AUCS = {
    "serving": {"roc_auc": 0.8819, "decidable_roc_auc": 0.8362, "forward_roc_auc": 0.8922},
    "profile": {"roc_auc": 0.8725, "decidable_roc_auc": 0.8236, "forward_roc_auc": 0.8901},
}


def sha12(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def resolve_inputs() -> Path:
    """Point LCP_DATASETS at verified inputs, downloading from the volume if needed."""
    if os.environ.get("LCP_DATASETS"):
        root = Path(os.environ["LCP_DATASETS"])
    else:
        root = CACHE
        root.mkdir(parents=True, exist_ok=True)
        missing = [n for n in INPUTS if not (root / n).exists()]
        if missing:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            for name in missing:
                print(f"downloading {VOLUME}/{name}")
                resp = w.files.download(f"{VOLUME}/{name}")
                (root / name).write_bytes(resp.contents.read())
    bad = {n: (sha12(root / n), want) for n, want in INPUTS.items() if sha12(root / n) != want}
    assert not bad, f"inputs in {root} do not match the 08-14 run (got, want): {bad}"
    os.environ["LCP_DATASETS"] = str(root)
    print(f"inputs verified in {root}")
    return root


def check_scores() -> int:
    import mlflow
    import numpy as np
    import pandas as pd

    sys.path.insert(0, str(HERE))
    sys.path.insert(0, str(HERE.parent.parent / "lib"))
    import databricks_conn
    import harness
    import score_population as sp_mod
    from mvp_model import CLAIM_MASKS

    mlflow.set_tracking_uri(harness.MLFLOW_TRACKING_URI)
    mlflow.set_registry_uri("databricks-uc")
    serving = mlflow.sklearn.load_model(SERVING_URI)
    profile = mlflow.sklearn.load_model(PROFILE_URI)

    ws, cols = sp_mod.attach_families(pd.read_parquet(harness.WORKING_SET))
    sp, score_cols = sp_mod.attach_families(pd.read_parquet(sp_mod.SCORING_POP))
    assert score_cols == cols

    # The training frames are built only for their categorical vocabularies; nothing is fitted.
    x_train, _ = sp_mod.build_frame(ws, cols)
    x_score = sp_mod.align_categories(x_train, sp_mod.build_frame(sp, cols)[0])
    p = serving.predict_proba(x_score)[:, 1]
    bands = sp_mod.cohort_bands(sp, p)
    expected, tier, bucket = sp_mod.calibrate(bands, bands["score_pct_within_cohort"])

    mask = CLAIM_MASKS[sp_mod.PROFILE_CLAIM]
    x_train_p, _ = sp_mod.build_frame(ws, cols, mask)
    x_score_p = sp_mod.align_categories(x_train_p, sp_mod.build_frame(sp, cols, mask)[0])
    p_p = profile.predict_proba(x_score_p)[:, 1]
    bands_p = sp_mod.cohort_bands(sp, p_p, by_office=False)
    oof_p = pd.read_parquet(harness.DATASETS / PROFILE_OOF)
    expected_p, _, bucket_p = sp_mod.calibrate(
        bands_p, bands_p["score_pct_within_cohort"], mask=mask, by_office=False, oof=oof_p
    )

    got = pd.DataFrame(
        {
            "campaign_id": sp["campaign_id"].to_numpy(),
            "p_corroborated": p.round(6),
            "expected_corroboration_rate": np.round(expected, 4),
            "tier": tier.to_numpy(),
            "score_bucket": bucket.to_numpy(),
            "band": bands["band"].to_numpy(),
            "score_pct_within_cohort": bands["score_pct_within_cohort"].to_numpy(),
            "cohort_key": bands["cohort_key"].to_numpy(),
            "profile_p_corroborated": p_p.round(6),
            "profile_expected_corroboration_rate": np.round(expected_p, 4),
            "profile_score_bucket": bucket_p.to_numpy(),
            "profile_score_pct_within_cohort": bands_p["score_pct_within_cohort"].to_numpy(),
            "profile_cohort_key": bands_p["cohort_key"].to_numpy(),
        }
    )
    want = databricks_conn.run_query(f"select {', '.join(got.columns)} from {SCORES_TABLE}")
    print(f"\nreproduced {len(got):,} rows; {SCORES_TABLE} has {len(want):,}")
    m = got.merge(want, on="campaign_id", how="outer", suffixes=("", "_table"), indicator=True)
    unmatched = int((m["_merge"] != "both").sum())
    print(f"  campaigns on one side only: {unmatched}")
    m = m[m["_merge"] == "both"]

    failures = unmatched
    for c in got.columns.drop("campaign_id"):
        a, b = m[c], m[f"{c}_table"]
        if pd.api.types.is_numeric_dtype(a):
            diff = ~np.isclose(a.astype(float), b.astype(float), atol=1e-9, equal_nan=True)
        else:
            diff = a.astype(str).ne(b.astype(str))
        print(f"  {'OK  ' if not diff.any() else 'FAIL'} {c}: {int(diff.sum())} rows differ")
        failures += int(diff.sum())
    print("\nscores REPRODUCE exactly" if not failures else f"\n{failures} differences")
    return 0 if not failures else 1


def check_aucs(profile: bool) -> int:
    from sklearn.ensemble import HistGradientBoostingClassifier

    sys.path.insert(0, str(HERE))
    import exp_family
    import harness
    from mvp_model import CLAIM_MASKS, COMPOSED_V2, FAMILY_SPECS, engineer, recency_weights

    df, cols_by_family = exp_family.load({f: FAMILY_SPECS[f] for f in COMPOSED_V2})
    cols = [c for fam_cols in cols_by_family.values() for c in fam_cols]
    eng = engineer(df)
    y = eng["corroborated"].astype(int)
    w = recency_weights(eng)

    failures = 0
    for name in ("serving", "profile") if profile else ("serving",):
        mask = CLAIM_MASKS["sales_profile"] if name == "profile" else ()
        x = exp_family.frame(df, eng, cols, mask)
        got = harness.evaluate(y, exp_family.oof_scores(x, y, eng["fold"], w), eng["decidable"])
        tr, te = eng["forward_role"].eq("train_2025"), eng["forward_role"].eq("test_2026plus")
        fwd = HistGradientBoostingClassifier(**exp_family.HGB).fit(
            x[tr], y[tr], sample_weight=w[tr.to_numpy()]
        )
        got["forward_roc_auc"] = harness.evaluate(y[te], fwd.predict_proba(x[te])[:, 1])["roc_auc"]
        print(f"\n{name} ({x.shape[1]} features, n={got['n']:,}, forward n={int(te.sum()):,})")
        for k, want in EXPECTED_AUCS[name].items():
            ok = abs(got[k] - want) < 5e-5
            failures += not ok
            print(f"  {'OK  ' if ok else 'FAIL'} {k}: {got[k]:.4f} (SESSION_NOTES {want:.4f})")
    return 0 if not failures else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("check", choices=["scores", "aucs"])
    ap.add_argument("--profile", action="store_true", help="aucs: also evaluate the profile tier")
    args = ap.parse_args()
    resolve_inputs()
    sys.exit(check_scores() if args.check == "scores" else check_aucs(args.profile))
