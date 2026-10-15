"""DATA-2225 MVP robustness: feature-group ablations on the HGB winner.

Question: how much of the AUC rides on features that may proxy BR *coverage*
(position type, state, BR arrivals) rather than candidate legitimacy?
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
from mvp_model import CAT_COLS, NUM_COLS, engineer, make_hgb_frame  # noqa: E402

ABLATIONS = {
    "full": [],
    "- br_normalized_position_type": ["br_normalized_position_type"],
    "- position_type, state": ["br_normalized_position_type", "campaign_state"],
    "- n_br_by_deadline (+flags)": ["n_br_by_deadline", "any_br_by_deadline", "br_arrivals_missing"],
    "- all coverage proxies": [
        "br_normalized_position_type",
        "campaign_state",
        "n_br_by_deadline",
        "any_br_by_deadline",
        "br_arrivals_missing",
    ],
}


def main() -> None:
    df = engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet"))
    y = df["corroborated"].astype(int)
    groups = df["user_id"]
    x_all = make_hgb_frame(df)
    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)
    splits = list(cv.split(x_all, y, groups))

    rows = []
    for name, drop in ABLATIONS.items():
        cols = [c for c in CAT_COLS + NUM_COLS if c not in drop]
        x = x_all[cols]
        oof = np.zeros(len(df))
        for tr, va in splits:
            m = HistGradientBoostingClassifier(
                categorical_features="from_dtype",
                random_state=7,
                max_iter=400,
                early_stopping=True,
                validation_fraction=0.15,
            ).fit(x.iloc[tr], y.iloc[tr])
            oof[va] = m.predict_proba(x.iloc[va])[:, 1]
        rows.append(
            {
                "ablation": name,
                "roc_auc": round(roc_auc_score(y, oof), 4),
                "pr_auc": round(average_precision_score(y, oof), 4),
            }
        )
        print(rows[-1])

    print()
    print(pd.DataFrame(rows).to_string(index=False))


if __name__ == "__main__":
    main()
