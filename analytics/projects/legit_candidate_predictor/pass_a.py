"""DATA-2225 pass A: add ALL product-behavior signals to the pass-B feature set.

Per the brief's two-pass leakage design: activity is multi-causal with the outcome
and these are LIFETIME aggregates (not as-of-deadline snapshots), so pass A measures
how much signal behavior carries — an upper bound, a diagnostic, never the analysis
filter. Pass B remains the filter.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.model_selection import StratifiedGroupKFold

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from mvp_model import engineer, make_hgb_frame  # noqa: E402

ACTIVITY_SQL = """
select
  b.user_id,
  b.is_activated,
  b.total_campaigns_sent,
  b.total_recipient_count,
  b.dashboard_view_count,
  b.first_dashboard_viewed_at,
  b.is_pro,
  b.has_pledged_campaign,
  b.has_verified_campaign,
  b.campaign_count,
  b.active_campaign_count,
  m.pro_upgrade_completed_at
from goodparty_data_catalog.mart_analytics.users_win_base b
left join goodparty_data_catalog.dbt.int__amplitude_user_milestones m
  on m.user_id = b.user_id
"""

ACT_NUM = [
    "log_campaigns_sent",
    "log_recipients",
    "log_dash_views",
    "campaign_count",
    "active_campaign_count",
    "viewed_dashboard",
    "act_is_activated",
    "act_is_pro",
    "act_pledged",
    "act_verified",
    "ever_pro_upgrade",
]


def build_activity(df: pd.DataFrame) -> pd.DataFrame:
    cache = HERE / "datasets" / "activity.parquet"
    if cache.exists():
        act = pd.read_parquet(cache)
    else:
        from databricks_conn import run_query

        act = run_query(ACTIVITY_SQL)
        act.to_parquet(cache, index=False)
    out = df.merge(act, on="user_id", how="left", suffixes=("", "_act"))
    assert len(out) == len(df), "activity merge fanned out"
    out["log_campaigns_sent"] = np.log1p(out["total_campaigns_sent"].fillna(0).astype(float))
    out["log_recipients"] = np.log1p(out["total_recipient_count"].fillna(0).astype(float))
    out["log_dash_views"] = np.log1p(out["dashboard_view_count"].fillna(0).astype(float))
    out["viewed_dashboard"] = out["first_dashboard_viewed_at"].notna().astype(int)
    out["act_is_activated"] = out["is_activated"].fillna(False).astype(int)
    out["act_is_pro"] = (
        out["is_pro_act"].fillna(False).astype(int)
        if "is_pro_act" in out
        else out["is_pro"].fillna(False).astype(int)
    )
    out["act_pledged"] = out["has_pledged_campaign"].fillna(False).astype(int)
    out["act_verified"] = out["has_verified_campaign"].fillna(False).astype(int)
    out["campaign_count"] = out["campaign_count"].fillna(0).astype(float)
    out["active_campaign_count"] = out["active_campaign_count"].fillna(0).astype(float)
    out["ever_pro_upgrade"] = out["pro_upgrade_completed_at"].notna().astype(int)
    return out


def run_cv(x, y, groups):
    cv = StratifiedGroupKFold(n_splits=5, shuffle=True, random_state=7)
    oof = np.zeros(len(y))
    last = None
    for tr, va in cv.split(x, y, groups):
        m = HistGradientBoostingClassifier(
            categorical_features="from_dtype",
            random_state=7,
            max_iter=400,
            early_stopping=True,
            validation_fraction=0.15,
        ).fit(x.iloc[tr], y.iloc[tr])
        oof[va] = m.predict_proba(x.iloc[va])[:, 1]
        last = (tr, va, m)
    return oof, last


if __name__ == "__main__":
    df = build_activity(engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet")))
    y = df["corroborated"].astype(int)
    groups = df["user_id"]

    x_b = make_hgb_frame(df)  # pass B features
    x_a = pd.concat([x_b, df[ACT_NUM]], axis=1)  # + activity

    for name, x in (
        ("pass B (no activity)", x_b),
        ("pass A (with activity)", x_a),
        ("activity ONLY", df[ACT_NUM]),
    ):
        oof, last = run_cv(x, y, groups)
        print(f"{name}: ROC-AUC={roc_auc_score(y, oof):.4f}  PR-AUC={average_precision_score(y, oof):.4f}")
        if name.startswith("pass A"):
            prec, rec, _ = precision_recall_curve(y, oof)
            for r_t in (0.95, 0.8, 0.5):
                print(f"  precision @ recall>={r_t}: {prec[rec >= r_t].max():.3f}")
            tr, va, m = last
            pi = permutation_importance(
                m, x.iloc[va], y.iloc[va], scoring="roc_auc", n_repeats=5, random_state=7
            )
            imp = pd.Series(pi.importances_mean, index=x.columns).sort_values(ascending=False)
            print("  top-10 importances (pass A):")
            print(imp.head(10).round(4).to_string())
