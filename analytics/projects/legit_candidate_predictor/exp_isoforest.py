"""Model-family experiment: isolation-forest weirdness score as an extra HGB feature.

Per frozen fold: fit IsolationForest on the TRAIN side only (all-numeric matrix from
the base allowlist: one-hot CAT_COLS min_frequency=30, median-impute NUM_COLS),
score train + validation, append the score as one numeric feature, retrain the
standard HGB. Also reports the weirdness score's own discrimination (negated
score_samples vs corroborated). No ledger writes; OOF saved to oof_isoforest.parquet.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import OneHotEncoder

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer, make_hgb_frame  # noqa: E402

EXP = "isolation-forest weirdness feature"
BASELINE = {"roc_auc": 0.8695, "decidable_roc_auc": 0.8172}

df = pd.read_parquet(harness.WORKING_SET)
folds = harness.load_folds()
df = df.merge(
    folds[["campaign_id", "fold", "forward_role", "decidable"]],
    on="campaign_id",
    how="left",
    validate="one_to_one",
)
assert df["fold"].notna().all()

eng = engineer(df)
y = eng["corroborated"].astype(int)


def make_numeric_pre() -> ColumnTransformer:
    return ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=30), CAT_COLS),
            ("num", SimpleImputer(strategy="median"), NUM_COLS),
        ]
    )


x_base = eng[CAT_COLS + NUM_COLS]
oof = np.zeros(len(eng))
weird_oof = np.zeros(len(eng))  # OOF weirdness: each row scored by a forest that never saw it

for k in sorted(eng["fold"].unique()):
    tr, va = eng["fold"].ne(k), eng["fold"].eq(k)

    pre = make_numeric_pre().fit(x_base[tr])
    iso = IsolationForest(n_estimators=300, random_state=7).fit(pre.transform(x_base[tr]))
    w_tr = iso.score_samples(pre.transform(x_base[tr]))
    w_va = iso.score_samples(pre.transform(x_base[va]))
    weird_oof[va.to_numpy()] = w_va

    x_hgb = make_hgb_frame(eng)
    x_hgb["weirdness"] = np.nan
    x_hgb.loc[tr, "weirdness"] = w_tr
    x_hgb.loc[va, "weirdness"] = w_va

    hgb = HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    ).fit(x_hgb[tr], y[tr])
    oof[va.to_numpy()] = hgb.predict_proba(x_hgb[va])[:, 1]
    print(f"fold {k} done  (weirdness-alone val AUC " f"{roc_auc_score(y[va], -w_va):.4f})")

metrics = harness.evaluate(y, oof, eng["decidable"])
print("\n=== base + weirdness (HGB, frozen OOF) ===")
print(metrics)
print(f"baseline: roc_auc {BASELINE['roc_auc']}  decidable {BASELINE['decidable_roc_auc']}")
print(
    f"delta:    roc_auc {metrics['roc_auc'] - BASELINE['roc_auc']:+.4f}  "
    f"decidable {metrics['decidable_roc_auc'] - BASELINE['decidable_roc_auc']:+.4f}"
)

# Standalone discrimination of the OOF weirdness score (more anomalous = higher pred)
d = eng["decidable"].to_numpy(dtype=bool)
weird_auc = roc_auc_score(y, -weird_oof)
weird_auc_dec = roc_auc_score(y[d], -weird_oof[d])
print("\n=== standalone weirdness (negated score_samples, OOF) ===")
print(f"overall ROC-AUC:   {weird_auc:.4f}")
print(f"decidable ROC-AUC: {weird_auc_dec:.4f}")

out = eng[["user_id", "campaign_id", "corroborated", "fold"]].copy()
out["decidable"] = eng["decidable"].to_numpy(dtype=bool)
out["oof_hgb_weird"] = oof
out["oof_weirdness"] = weird_oof
out.to_parquet(HERE / "datasets" / "oof_isoforest.parquet", index=False)
print("\nsaved datasets/oof_isoforest.parquet")
