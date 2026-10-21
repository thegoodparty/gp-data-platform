"""DATA-2238 Area 1: label-robustness rerun by EXCLUDING verified-real rows from training.

The web verification found 508 rows labelled `corroborated=0` that are demonstrably real
candidates. The tempting move is to relabel them to 1, which is exactly wrong: 419 of them were
selected for verification *because the model scored them high*, so flipping their labels hands the
model back its own opinion and the AUC rises for no reason. Exclusion is the honest version of the
same experiment. It asks a question with a real answer: does dropping known-bad labels from
TRAINING make the model better on the rows we still trust?

Design, so control and variant are comparable:
  - both arms are evaluated on the SAME held-out population (every row minus the excluded set)
  - only the training folds differ
  - the excluded rows never appear in either arm's evaluation, so their own labels cannot move
    the metric in either direction

Two exclusion sets, because they carry different amounts of circularity:
  all      508 verified-real across every wave. 419 come from the adversarially-selected sweep,
           so this arm is contaminated by construction and is reported with that label attached.
  design    69 verified-real from the stratified wave only. Score-stratified with a uniform draw
           per bin, so it is not a hard-case selection; this is the arm to believe.

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/exp_label_exclusion.py --ledger
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer, recency_weights  # noqa: E402

DATASETS = HERE / "datasets"
WS_FILE = DATASETS / "working_set_v2.parquet"
WS_VERSION = "b04cdd5ac4b1"
FOLDS = DATASETS / "folds_b04cdd5ac4b1.parquet"
VERDICTS = DATASETS / "web_verify_verdicts_snapshot.parquet"
REAL = ("CONFIRMED", "LIKELY")

# DATA-2312 dropped election_level: it is 100% missing for 2026 and acts as an era marker rather
# than a level signal. The canonical manifest c1f1d210c9a5 carries the same feature list.
CANON_CAT = [c for c in CAT_COLS if c != "election_level"]
HGB = dict(
    categorical_features="from_dtype",
    random_state=7,
    max_iter=400,
    early_stopping=True,
    validation_fraction=0.15,
)
# Ledger row "ws-rebuild pass 3 CANONICAL", model c1f1d210c9a5.
CANON = {"roc_auc": 0.8732, "decidable_roc_auc": 0.8180}


def canon_frame(eng: pd.DataFrame) -> pd.DataFrame:
    x = eng[CANON_CAT + NUM_COLS].copy()
    for c in CANON_CAT:
        x[c] = x[c].astype("category")
    return x


def oof_with_holdout(
    x: pd.DataFrame, y: pd.Series, fold: pd.Series, w: np.ndarray, train_mask: pd.Series
) -> np.ndarray:
    """Out-of-fold scores where `train_mask` restricts who may TRAIN. Everyone is still scored."""
    oof = np.zeros(len(x))
    for k in sorted(fold.unique()):
        tr = fold.ne(k) & train_mask
        va = fold.eq(k)
        m = HistGradientBoostingClassifier(**HGB).fit(x[tr], y[tr], sample_weight=w[tr.to_numpy()])
        oof[va.to_numpy()] = m.predict_proba(x[va])[:, 1]
    return oof


def main(ledger: bool) -> None:
    assert harness.ws_version(WS_FILE) == WS_VERSION, "not the DATA-2312 canonical working set"
    ws = pd.read_parquet(WS_FILE)
    folds = pd.read_parquet(FOLDS)
    df = ws.merge(
        folds[["campaign_id", "fold", "forward_role", "decidable"]],
        on="campaign_id",
        how="left",
        validate="one_to_one",
    )
    assert df["fold"].notna().all()

    eng = engineer(df)
    y = eng["corroborated"].astype(int)
    fold = eng["fold"]
    w = recency_weights(eng)
    x = canon_frame(eng)

    ver = pd.read_parquet(VERDICTS)
    real_ids = {
        "all": set(ver.loc[ver["verdict"].isin(REAL), "campaign_id"]),
        "design": set(
            ver.loc[ver["verdict"].isin(REAL) & ver["wave"].isin(("stratified", "mopup")), "campaign_id"]
        ),
    }
    for name, ids in real_ids.items():
        print(f"{name:7s}: {len(ids):>4} verified-real campaign_ids")

    everyone = pd.Series(True, index=eng.index)
    control = oof_with_holdout(x, y, fold, w, everyone)
    ctl_full = harness.evaluate(y, control, eng["decidable"])
    print(
        f"\ncontrol reproduces canonical: roc_auc {ctl_full['roc_auc']:.4f} "
        f"(ledger {CANON['roc_auc']:.4f}, delta {ctl_full['roc_auc'] - CANON['roc_auc']:+.4f})"
    )
    assert abs(ctl_full["roc_auc"] - CANON["roc_auc"]) < 5e-3, (
        "control does not reproduce the canonical baseline; the feature path has drifted and no "
        "delta below is interpretable"
    )

    rows = []
    for name, ids in real_ids.items():
        excluded = eng["campaign_id"].isin(ids)
        keep = ~excluded  # the shared evaluation population for BOTH arms
        variant = oof_with_holdout(x, y, fold, w, keep)

        ctl_m = harness.evaluate(y[keep], control[keep.to_numpy()], eng.loc[keep, "decidable"])
        var_m = harness.evaluate(y[keep], variant[keep.to_numpy()], eng.loc[keep, "decidable"])
        rows.append(
            {
                "exclusion": name,
                "n_excluded": int(excluded.sum()),
                "eval_n": ctl_m["n"],
                "control_roc": ctl_m["roc_auc"],
                "excluded_roc": var_m["roc_auc"],
                "delta_roc": round(var_m["roc_auc"] - ctl_m["roc_auc"], 4),
                "control_dec": ctl_m["decidable_roc_auc"],
                "excluded_dec": var_m["decidable_roc_auc"],
                "delta_dec": round(var_m["decidable_roc_auc"] - ctl_m["decidable_roc_auc"], 4),
            }
        )

    out = pd.DataFrame(rows)
    print("\n=== exclusion rerun: same eval population, training differs ===")
    print(out.to_string(index=False))
    print(
        "\n'all' is contaminated: 419 of its exclusions were chosen for scoring high, so any "
        "gain is partly the model being spared its own hard cases. 'design' is the honest arm."
    )

    if ledger:
        d = out.set_index("exclusion")
        harness.append_ledger(
            "label exclusion rerun (verified-real dropped from training)",
            "oof_cv5",
            "hgb",
            {
                "n_excluded_all": int(d.loc["all", "n_excluded"]),
                "n_excluded_design": int(d.loc["design", "n_excluded"]),
                "delta_roc_all": float(d.loc["all", "delta_roc"]),
                "delta_roc_design": float(d.loc["design", "delta_roc"]),
                "delta_decidable_all": float(d.loc["all", "delta_dec"]),
                "delta_decidable_design": float(d.loc["design", "delta_dec"]),
                "control_roc_auc": float(ctl_full["roc_auc"]),
            },
            notes="DATA-2238. Exclusion, not relabel: relabelling model-selected rows returns the "
            "model its own opinion. Both arms scored on the same population (all rows minus "
            "the excluded set); only training membership differs. The 'all' arm is "
            "adversarially contaminated by the sweep's selection; believe the 'design' arm.",
            area="area1",
            ver=WS_VERSION,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", action="store_true")
    main(ap.parse_args().ledger)
