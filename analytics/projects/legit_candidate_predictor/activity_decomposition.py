"""DATA-2225 diagnostic: why does activity (pass A) add so little over pass B?

Decomposes the pass-A vs pass-B OOF gap by an ACTIVE / INACTIVE split of the pool.
If the pass-A lift is concentrated in the active subset, activity is informative but
diluted by the mostly-inactive pool; if it is absent even there, activity is genuinely
redundant with signup-time structure.

Frozen folds (harness.load_folds), sentinel-aware engineer(), same HGB spec as v1.
Read-only diagnostic: does NOT append to results_ledger.jsonl.

Active definition: any genuine usage count > 0, i.e.
    total_campaigns_sent > 0  OR  total_recipient_count > 0  OR  dashboard_view_count > 0
(campaign_count is structural — 100% of the pool has it > 0 — and
active_campaign_count / has_pledged_campaign / has_verified_campaign are near-universal
state flags at 91% / 92% / 74%, so including any of them would make the split degenerate.)

Run from analytics/:
    uv run --with scikit-learn --with pyarrow python \
        projects/legit_candidate_predictor/activity_decomposition.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harness  # noqa: E402
from mvp_model import engineer, make_hgb_frame  # noqa: E402
from pass_a import ACT_NUM, build_activity  # noqa: E402

PASS_B_CANONICAL = 0.8695
TOLERANCE = 0.002

USAGE_COUNT_COLS = ["total_campaigns_sent", "total_recipient_count", "dashboard_view_count"]


def make_model() -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(
        categorical_features="from_dtype",
        random_state=7,
        max_iter=400,
        early_stopping=True,
        validation_fraction=0.15,
    )


def main() -> None:
    df = build_activity(engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet")))
    y = df["corroborated"].astype(int)

    active = df[USAGE_COUNT_COLS].fillna(0).astype(float).gt(0).any(axis=1)
    print("ACTIVE definition: any of " f"{USAGE_COUNT_COLS} > 0")
    print(f"n={len(df):,}  share active={active.mean():.4f} (n={int(active.sum()):,})")
    print(f"positive rate | active:   {y[active].mean():.4f}")
    print(f"positive rate | inactive: {y[~active].mean():.4f}")

    # Frozen folds, joined on campaign_id (unique key per harness.freeze_folds).
    folds = harness.load_folds()
    df = df.merge(folds[["campaign_id", "fold"]], on="campaign_id", how="left", validate="one_to_one")
    assert df["fold"].notna().all(), "working set rows missing a frozen fold"
    fold = df["fold"].astype(int)

    x_b = make_hgb_frame(df)  # pass B: CAT_COLS + NUM_COLS
    x_a = pd.concat([x_b, df[ACT_NUM]], axis=1)  # pass A: + activity

    oof = {"pass_b": np.zeros(len(df)), "pass_a": np.zeros(len(df))}
    last_a = None
    for k in sorted(fold.unique()):
        tr, va = fold.ne(k).to_numpy(), fold.eq(k).to_numpy()
        for name, x in (("pass_b", x_b), ("pass_a", x_a)):
            m = make_model().fit(x[tr], y[tr])
            oof[name][va] = m.predict_proba(x[va])[:, 1]
            if name == "pass_a":
                last_a = (va, m)

    # Sanity gate: pass-B rerun must land near the canonical sentinel-aware number.
    auc_b = roc_auc_score(y, oof["pass_b"])
    print(f"\npass B OOF ROC-AUC rerun: {auc_b:.4f} (canonical {PASS_B_CANONICAL})")
    if abs(auc_b - PASS_B_CANONICAL) > TOLERANCE:
        raise SystemExit(
            f"STOP: pass-B rerun {auc_b:.4f} differs from canonical "
            f"{PASS_B_CANONICAL} by more than {TOLERANCE}; do not trust the decomposition."
        )

    # Decomposition table: overall / active / inactive x pass B / pass A.
    rows = []
    subsets = {"overall": pd.Series(True, index=df.index), "active": active, "inactive": ~active}
    for sub_name, mask in subsets.items():
        row = {"subset": sub_name, "n": int(mask.sum()), "pos_rate": round(y[mask].mean(), 4)}
        for p_name in ("pass_b", "pass_a"):
            row[f"{p_name}_auc"] = round(roc_auc_score(y[mask], oof[p_name][mask]), 4)
        row["delta_a_minus_b"] = round(row["pass_a_auc"] - row["pass_b_auc"], 4)
        rows.append(row)
    table = pd.DataFrame(rows)
    print("\n=== OOF ROC-AUC decomposition ===")
    print(table.to_string(index=False))

    # Which activity carries the signal (if any): permutation importance of the
    # last fold's pass-A model, scored on the ACTIVE rows of its validation split.
    va, model_a = last_a
    va_active = va & active.to_numpy()
    print(
        f"\npermutation importance: last fold val, ACTIVE rows only "
        f"(n={int(va_active.sum())}, pos rate={y[va_active].mean():.3f})"
    )
    pi = permutation_importance(
        model_a, x_a[va_active], y[va_active], scoring="roc_auc", n_repeats=5, random_state=7
    )
    imp = pd.Series(pi.importances_mean, index=x_a.columns)
    print("top 5 ACT_* features (ROC-AUC drop):")
    print(imp[ACT_NUM].sort_values(ascending=False).head(5).round(4).to_string())

    out = df[["campaign_id", "user_id", "corroborated", "fold"]].copy()
    out["active"] = active.values
    out["oof_pass_b"] = oof["pass_b"]
    out["oof_pass_a"] = oof["pass_a"]
    out_path = HERE / "datasets" / "oof_activity_decomposition.parquet"
    out.to_parquet(out_path, index=False)
    print(f"\nsaved {out_path.name} (n={len(out):,})")


if __name__ == "__main__":
    main()
