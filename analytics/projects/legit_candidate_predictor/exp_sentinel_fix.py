"""First ledger experiment: sentinel-aware n_br_by_deadline (Area 0b fix).

engineer() now NaNs by_deadline=0 when no deadline exists (4,455 sentinel rows) and
lets any_br_by_deadline be unknown wherever the count is. Retrains the v1 models on
the FROZEN folds and appends to the ledger; v1 caches (oof_predictions.parquet,
results.json, joblibs) are left untouched.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer, make_hgb_frame, make_lr  # noqa: E402

EXP = "sentinel-aware n_br_by_deadline"

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

# sanity: the fix moves effective-unknown to the audit's 55.9%
miss_rate = eng["br_arrivals_missing"].mean()
print(f"br_arrivals_missing after fix: {miss_rate:.4f} (audit expectation 0.5591)")
assert abs(miss_rate - 0.5591) < 1e-3

x_hgb = make_hgb_frame(eng)
x_lr = eng[CAT_COLS + NUM_COLS]

oof = {"hgb": np.zeros(len(eng)), "lr": np.zeros(len(eng))}
for k in sorted(eng["fold"].unique()):
    tr, va = eng["fold"].ne(k), eng["fold"].eq(k)
    hgb = HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    ).fit(x_hgb[tr], y[tr])
    oof["hgb"][va] = hgb.predict_proba(x_hgb[va])[:, 1]
    lr = make_lr().fit(x_lr[tr], y[tr])
    oof["lr"][va] = lr.predict_proba(x_lr[va])[:, 1]
    print(f"fold {k} done")

for m in ("hgb", "lr"):
    metrics = harness.evaluate(y, oof[m], eng["decidable"])
    harness.append_ledger(
        EXP,
        "oof_cv5",
        m,
        metrics,
        area="0b",
        notes="mvp_model.engineer fix: NaN sentinel zeros + unknown any_br; "
        "vs v1 baseline hgb 0.875 / lr 0.857",
    )
    print(m, metrics)

fwd_tr, fwd_te = eng["forward_role"].eq("train_2025"), eng["forward_role"].eq("test_2026plus")
hgb = HistGradientBoostingClassifier(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
).fit(x_hgb[fwd_tr], y[fwd_tr])
fwd = harness.evaluate(y[fwd_te], hgb.predict_proba(x_hgb[fwd_te])[:, 1])
harness.append_ledger(EXP, "forward_2025to2026", "hgb", fwd, area="0b", notes="vs v1 forward 0.896")
print("forward", fwd)

out = eng[["user_id", "campaign_id", "corroborated"]].copy()
out["oof_lr"], out["oof_hgb"] = oof["lr"], oof["hgb"]
out.to_parquet(HERE / "datasets" / "oof_sentinel_fix.parquet", index=False)
print("saved datasets/oof_sentinel_fix.parquet")
