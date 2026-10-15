"""Reproduce the 2026-08-07 training-window / recency-weighting sweep (ledger area4 row).

The sweep script was not kept; before folding recency weights into the canonical
baseline we re-derive its numbers on the frozen forward split to pin the method:
which clock column, which window semantics, which anchor. Targets (ledger):

  window_2025plus 0.8894 | window_2024plus 0.8884 | window_2025q4 0.7165
  recency_hl120 0.8950 / PR 0.9136 | hl240 0.8933 | hl480 0.8919
  mean_pred_2025plus 0.556 | mean_pred_recency_hl120 0.515

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow python \
      projects/legit_candidate_predictor/exp_recency_reproduce.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import engineer, make_hgb_frame  # noqa: E402

HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
)

df = pd.read_parquet(harness.WORKING_SET)
folds = harness.load_folds()
df = df.merge(folds[["campaign_id", "forward_role"]], on="campaign_id", how="left", validate="one_to_one")
eng = engineer(df)
y = eng["corroborated"].astype(int)
x = make_hgb_frame(eng)
role = eng["forward_role"]
tr_all, te = role.eq("train_2025"), role.eq("test_2026plus")
signup = pd.to_datetime(eng["signup_date"])
yte = y[te]
print(f"forward test n={int(te.sum())} base={yte.mean():.3f} train_all n={int(tr_all.sum())}")


def fit_eval(train_mask, weights=None, label=""):
    m = HistGradientBoostingClassifier(**HGB).fit(
        x[train_mask],
        y[train_mask],
        sample_weight=None if weights is None else weights[train_mask.to_numpy()],
    )
    p = m.predict_proba(x[te])[:, 1]
    auc, pr = roc_auc_score(yte, p), average_precision_score(yte, p)
    print(
        f"  {label:34s} n_train={int(train_mask.sum()):6d} "
        f"roc {auc:.4f}  pr {pr:.4f}  mean_pred {p.mean():.3f}"
    )
    return auc


print("\n=== window cuts on signup_date (unweighted) ===")
cuts = {
    "all train rows (canonical)": tr_all,
    "signup>=2024 (2024plus)": tr_all & (signup >= "2024-01-01"),
    "signup>=2025 (2025plus)": tr_all & (signup >= "2025-01-01"),
    "signup>=2025-04 (2025q2plus)": tr_all & (signup >= "2025-04-01"),
    "signup>=2025-07 (2025h2)": tr_all & (signup >= "2025-07-01"),
    "signup in 2025q4 only": tr_all & (signup >= "2025-10-01") & (signup < "2026-01-01"),
}
for label, mask in cuts.items():
    fit_eval(mask, label=label)

print("\n=== recency weights on signup_date, train = signup>=2024 ===")
base_mask = tr_all & (signup >= "2024-01-01")
anchor = signup[base_mask].max()
age = (anchor - signup).dt.days.clip(lower=0).to_numpy(dtype=float)
for hl in (120, 240, 480):
    w = np.power(0.5, age / hl)
    fit_eval(base_mask, weights=w, label=f"hl={hl}d (anchor={anchor.date()})")

print("\n=== same but clock on general_election_date (alternative hypothesis) ===")
elec = pd.to_datetime(eng["general_election_date"])
anchor_e = elec[base_mask].max()
age_e = (anchor_e - elec).dt.days.clip(lower=0).to_numpy(dtype=float)
for hl in (120,):
    w = np.power(0.5, age_e / hl)
    fit_eval(base_mask, weights=w, label=f"hl={hl}d on election_date")
