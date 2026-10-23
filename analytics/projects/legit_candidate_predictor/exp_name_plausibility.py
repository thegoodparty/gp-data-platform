"""Area 2 experiment: name plausibility features on top of the canonical pass-B baseline.

Adds the 14 features from build_name_plausibility.py (voter-file name frequency plus a-priori
structural flags) to the base allowlist and retrains the standard HGB on the frozen folds.

The features are label-free by construction (frequency comes from the voter file; the token
and fictional-name lists are a-priori) and none were selected on the label, so the frozen-fold
numbers are honest. Expectation going in: a small ROC-AUC move, because every flag bucket is
tiny. The value being tested is precision at the bottom of the score, so this also reports
precision in the lowest score decile, which is what a noise filter is actually used for.

Run: uv run --with scikit-learn --with pyarrow python \
       projects/legit_candidate_predictor/exp_name_plausibility.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import engineer, make_hgb_frame  # noqa: E402

EXP = "name plausibility features"
BASELINE = {"roc_auc": 0.8695, "pr_auc": 0.8844, "decidable_roc_auc": 0.8172, "forward_roc_auc": 0.8912}

df = pd.read_parquet(harness.WORKING_SET)
folds = harness.load_folds()
df = df.merge(
    folds[["campaign_id", "fold", "forward_role", "decidable"]],
    on="campaign_id",
    how="left",
    validate="one_to_one",
)
assert df["fold"].notna().all()

names = pd.read_parquet(HERE / "datasets" / "name_features.parquet")
name_cols = [c for c in names.columns if c != "user_id"]
before = len(df)
df = df.merge(names, on="user_id", how="left", validate="many_to_one")
assert len(df) == before, "name-feature merge changed row count"
print(f"name features attached: {df[name_cols[0]].notna().mean():.1%} of rows, " f"{len(name_cols)} columns")

eng = engineer(df)
y = eng["corroborated"].astype(int)

x = make_hgb_frame(eng)
for c in name_cols:
    x[c] = df[c].to_numpy()

oof = np.zeros(len(eng))
for k in sorted(eng["fold"].unique()):
    tr, va = eng["fold"].ne(k), eng["fold"].eq(k)
    hgb = HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    ).fit(x[tr], y[tr])
    oof[va.to_numpy()] = hgb.predict_proba(x[va])[:, 1]
    print(f"fold {k} done")

metrics = harness.evaluate(y, oof, eng["decidable"])
print(f"\n=== base + name plausibility (HGB, frozen OOF) ===\n{metrics}")
for key in ("roc_auc", "pr_auc", "decidable_roc_auc"):
    print(
        f"{key}: {metrics[key]:.4f}  baseline {BASELINE[key]:.4f}  "
        f"delta {metrics[key] - BASELINE[key]:+.4f}"
    )

# What the family is actually for: how clean is the bottom of the score?
base_oof = pd.read_parquet(HERE / "datasets" / "oof_sentinel_fix.parquet")
base_col = next(c for c in base_oof.columns if c.startswith("oof_hgb"))
cmp = eng[["campaign_id"]].copy()
cmp["y"] = y.to_numpy()
cmp["new"] = oof
cmp = cmp.merge(
    base_oof[["campaign_id", base_col]], on="campaign_id", how="inner", validate="one_to_one"
).rename(columns={base_col: "base"})
print(f"\n=== bottom-decile purity (n={len(cmp):,} matched to the baseline OOF) ===")
for col in ("base", "new"):
    cut = cmp[col].quantile(0.10)
    bottom = cmp[cmp[col] <= cut]
    print(
        f"{col:>5}: bottom 10% n={len(bottom):,}  corroboration {bottom['y'].mean():.1%} "
        f"(lower is better)"
    )

# Forward split, same hyperparameters and seed as every other ledger row.
role = eng["forward_role"]
tr, te = role.eq("train_2025"), role.eq("test_2026plus")
fwd_model = HistGradientBoostingClassifier(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
).fit(x[tr], y[tr])
p_fwd = fwd_model.predict_proba(x[te])[:, 1]
fwd = harness.evaluate(y[te], p_fwd)
print(f"\n=== forward 2025 -> 2026 ===\n{fwd}")
print(
    f"roc_auc {fwd['roc_auc']:.4f}  baseline {BASELINE['forward_roc_auc']:.4f}  "
    f"delta {fwd['roc_auc'] - BASELINE['forward_roc_auc']:+.4f}"
)

# Standalone discrimination of the family on its own, for the ledger note.
name_only = HistGradientBoostingClassifier(random_state=7, max_iter=200).fit(
    df.loc[eng["fold"].ne(0), name_cols], y[eng["fold"].ne(0)]
)
p_solo = name_only.predict_proba(df.loc[eng["fold"].eq(0), name_cols])[:, 1]
print(
    f"\nname features alone (fold 0 holdout): ROC-AUC " f"{roc_auc_score(y[eng['fold'].eq(0)], p_solo):.4f}"
)

out = eng[["user_id", "campaign_id", "corroborated", "fold"]].copy()
out["decidable"] = eng["decidable"].to_numpy(dtype=bool)
out["oof_hgb_name"] = oof
out.to_parquet(HERE / "datasets" / "oof_name_plausibility.parquet", index=False)
print("\nsaved datasets/oof_name_plausibility.parquet")
