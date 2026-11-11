"""DATA-2225 leakage probe: regroup CV folds by RACE instead of user.

Users in the same race share race-level features (n_br_by_deadline, deadline,
position type, voter_count). User-grouped folds let the same race appear in both
train and validation; if the model exploits that, user-grouped CV overstates
forward performance. Race-grouped CV closes the channel.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from mvp_model import engineer, make_hgb_frame  # noqa: E402

df = engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet"))
y = df["corroborated"].astype(int)
x = make_hgb_frame(df)

race_key = (
    df["campaign_state"].astype(str)
    + "|"
    + df["br_position_id"].astype(str)
    + "|"
    + df["general_election_date"].astype(str)
)
# rows with no BR position id fall back to a per-user group (they share no race key)
race_key = race_key.where(df["br_position_id"].notna(), "user_" + df["user_id"].astype(str))
print(f"rows={len(df):,}  distinct race groups={race_key.nunique():,}")

for name, groups in (("user-grouped (original)", df["user_id"]), ("race-grouped", race_key)):
    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)
    oof = np.zeros(len(df))
    for tr, va in cv.split(x, y, groups):
        m = HistGradientBoostingClassifier(
            categorical_features="from_dtype",
            random_state=7,
            max_iter=400,
            early_stopping=True,
            validation_fraction=0.15,
        ).fit(x.iloc[tr], y.iloc[tr])
        oof[va] = m.predict_proba(x.iloc[va])[:, 1]
    print(f"{name}: ROC-AUC={roc_auc_score(y, oof):.4f}  PR-AUC={average_precision_score(y, oof):.4f}")
