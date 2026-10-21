"""Area 2 experiment: BR position attributes on top of the canonical pass-B baseline.

Cost-of-entry features (filing fee, signature count, requirement-text length, minimum age, seats,
salary, election frequency) plus race-structure flags, merged on br_position_id. Adds the two
ratios that need the working set's district size, since a fee only means something relative to the
electorate it buys access to.

Reports the pooled, decidable, and forward numbers against the 0.8695 ledger baseline, plus the
family's standalone discrimination and permutation importance for the new columns only.

Run: uv run --with scikit-learn --with pyarrow python \
       projects/legit_candidate_predictor/exp_br_position.py
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

EXP = "BR position attributes"
BASELINE = {"roc_auc": 0.8695, "pr_auc": 0.8844, "decidable_roc_auc": 0.8172, "forward_roc_auc": 0.8912}
CAT_NEW = [
    "pos_salary_period",
    "pos_salary_class",
    "pos_employment_type",
    "pos_partisan_type",
    "pos_br_level",
]

df = pd.read_parquet(harness.WORKING_SET)
folds = harness.load_folds()
df = df.merge(
    folds[["campaign_id", "fold", "forward_role", "decidable"]],
    on="campaign_id",
    how="left",
    validate="one_to_one",
)
assert df["fold"].notna().all()

pos = pd.read_parquet(HERE / "datasets" / "br_position_features.parquet")
before = len(df)
df = df.merge(pos, on="br_position_id", how="left", validate="many_to_one")
assert len(df) == before, "position merge changed row count"

# Ratios that need the district size. Fee per registered voter is the honest cost-of-entry measure:
# $500 to reach 2,000 voters is a different proposition from $500 to reach 200,000.
df["pos_fee_per_1k_voters"] = np.where(
    df["pos_max_filing_fee"].notna() & df["voter_count"].gt(0),
    df["pos_max_filing_fee"] / (df["voter_count"] / 1000),
    np.nan,
)
df["pos_signatures_per_1k_voters"] = np.where(
    df["pos_signature_count"].notna() & df["voter_count"].gt(0),
    df["pos_signature_count"] / (df["voter_count"] / 1000),
    np.nan,
)

new_cols = [c for c in pos.columns if c != "br_position_id"] + [
    "pos_fee_per_1k_voters",
    "pos_signatures_per_1k_voters",
]
reach = df["pos_seats"].notna().mean()
print(f"position features attached to {reach:.1%} of rows, {len(new_cols)} columns")

eng = engineer(df)
y = eng["corroborated"].astype(int)

x = make_hgb_frame(eng)
for c in new_cols:
    if c in CAT_NEW:
        x[c] = pd.Categorical(df[c].astype("object").where(df[c].notna(), "missing"))
    else:
        x[c] = pd.to_numeric(df[c], errors="coerce").to_numpy()

oof = np.zeros(len(eng))
solo = np.zeros(len(eng))
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

    # The family on its own, out of fold, to see whether it carries signal independently.
    only = HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=300,
        early_stopping=True,
        validation_fraction=0.15,
    ).fit(x.loc[tr, new_cols], y[tr])
    solo[va.to_numpy()] = only.predict_proba(x.loc[va, new_cols])[:, 1]
    print(f"fold {k} done")

metrics = harness.evaluate(y, oof, eng["decidable"])
print(f"\n=== base + BR position attributes (HGB, frozen OOF) ===\n{metrics}")
for key in ("roc_auc", "pr_auc", "decidable_roc_auc"):
    print(
        f"{key}: {metrics[key]:.4f}  baseline {BASELINE[key]:.4f}  "
        f"delta {metrics[key] - BASELINE[key]:+.4f}"
    )
print(f"\nposition family ALONE (OOF): roc_auc {roc_auc_score(y, solo):.4f}")

role = eng["forward_role"]
tr, te = role.eq("train_2025"), role.eq("test_2026plus")
fwd_model = HistGradientBoostingClassifier(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
).fit(x[tr], y[tr])
fwd = harness.evaluate(y[te], fwd_model.predict_proba(x[te])[:, 1])
print(f"\n=== forward 2025 -> 2026 ===\n{fwd}")
print(
    f"roc_auc {fwd['roc_auc']:.4f}  baseline {BASELINE['forward_roc_auc']:.4f}  "
    f"delta {fwd['roc_auc'] - BASELINE['forward_roc_auc']:+.4f}"
)

# Which of the new columns actually did anything, on the last fold's model.
imp = pd.Series(
    fwd_model.feature_importances_ if hasattr(fwd_model, "feature_importances_") else np.zeros(x.shape[1]),
    index=x.columns,
)
if imp.abs().sum() == 0:  # HGB exposes no native importances; use a permutation on the new block
    from sklearn.inspection import permutation_importance

    r = permutation_importance(fwd_model, x[te], y[te], n_repeats=5, random_state=7, scoring="roc_auc")
    imp = pd.Series(r.importances_mean, index=x.columns)
print("\ntop 12 new-column importances (permutation, forward test):")
print(imp[[c for c in new_cols if c in imp.index]].sort_values(ascending=False).head(12).to_string())

out = eng[["user_id", "campaign_id", "corroborated", "fold"]].copy()
out["decidable"] = eng["decidable"].to_numpy(dtype=bool)
out["oof_hgb_pos"] = oof
out["oof_pos_only"] = solo
out.to_parquet(HERE / "datasets" / "oof_br_position.parquet", index=False)
print("\nsaved datasets/oof_br_position.parquet")
