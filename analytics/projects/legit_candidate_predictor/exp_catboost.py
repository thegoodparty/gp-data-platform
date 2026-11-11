"""Model-family experiment: CatBoost vs the HGB baseline (DATA-2225).

Same frozen folds, same sentinel-aware engineer(), EXACTLY CAT_COLS + NUM_COLS.
One small manual tweak round chosen on fold 0 only, then applied to all folds.
Does NOT touch the results ledger; OOF preds -> datasets/oof_catboost.parquet.
"""

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer  # noqa: E402

t0 = time.time()

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
x = eng[CAT_COLS + NUM_COLS].copy()
for c in NUM_COLS:
    x[c] = x[c].astype(float)  # NaN allowed; CatBoost handles numeric NaN natively


def fit_catboost(x_tr, y_tr, extra_params: dict) -> CatBoostClassifier:
    """Fit with early stopping on a 15% stratified holdout of the train fold."""
    xa, xb, ya, yb = train_test_split(x_tr, y_tr, test_size=0.15, stratify=y_tr, random_state=7)
    model = CatBoostClassifier(
        iterations=2000,
        random_seed=7,
        od_type="Iter",
        od_wait=100,
        eval_metric="AUC",
        cat_features=CAT_COLS,
        verbose=0,
        allow_writing_files=False,
        **extra_params,
    )
    model.fit(xa, ya, eval_set=(xb, yb))
    return model


# --- one manual tweak round, fold 0 only ---
tr0, va0 = eng["fold"].ne(0), eng["fold"].eq(0)
candidates = {
    "defaults (depth=6, auto lr)": {},
    "depth=8": {"depth": 8},
    "learning_rate=0.05": {"learning_rate": 0.05},
}
tweak = {}
for name, params in candidates.items():
    m = fit_catboost(x[tr0], y[tr0], params)
    auc = harness.evaluate(y[va0], m.predict_proba(x[va0])[:, 1])["roc_auc"]
    tweak[name] = (auc, params, m.tree_count_)
    print(f"fold-0 tweak  {name:<28} val roc_auc={auc:.4f}  trees={m.tree_count_}")

best_name = max(tweak, key=lambda k: tweak[k][0])
best_params = tweak[best_name][1]
print(f"chosen: {best_name}")

# --- all frozen folds with the chosen settings ---
oof = np.zeros(len(eng))
for k in sorted(eng["fold"].unique()):
    tr, va = eng["fold"].ne(k), eng["fold"].eq(k)
    m = fit_catboost(x[tr], y[tr], best_params)
    oof[va] = m.predict_proba(x[va])[:, 1]
    print(f"fold {k} done (trees={m.tree_count_})")

metrics = harness.evaluate(y, oof, eng["decidable"])
print("\nOOF:", metrics)

# --- forward split ---
fwd_tr = eng["forward_role"].eq("train_2025")
fwd_te = eng["forward_role"].eq("test_2026plus")
m = fit_catboost(x[fwd_tr], y[fwd_tr], best_params)
fwd = harness.evaluate(y[fwd_te], m.predict_proba(x[fwd_te])[:, 1])
print("forward:", fwd)

out = eng[["campaign_id", "corroborated"]].copy()
out["oof_catboost"] = oof
out.to_parquet(HERE / "datasets" / "oof_catboost.parquet", index=False)
print(f"saved datasets/oof_catboost.parquet  ({time.time() - t0:.0f}s total)")
