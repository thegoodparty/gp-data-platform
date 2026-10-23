"""DATA-2238 Area 1: re-run confident learning on the DATA-2312 working set, and finally TEST it.

The 2026-08-06 cleanlab pass ran on the pre-rebuild working set and its output was taken on faith:
a ranked list of probably-mislabeled negatives with no way to check whether the ranking was right.
Two things changed. The working set was rebuilt (ws b04cdd5ac4b1), so the flags need recomputing;
and 150 negatives were verified under a probability design, so the flags can now be scored against
ground truth instead of assumed.

The evaluation has to be done on the DESIGN sample and nowhere else. The sweep wave verified the
old high-confidence flag set itself, so measuring the flags against the sweep asks whether the
flags agree with the set the flags chose. The stratified wave was drawn independently of any flag,
which makes it the only fair test, at the cost of a small n.

Run (from analytics/):
  uv run --with cleanlab --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/cleanlab_rerun.py --ledger
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from cleanlab.filter import find_label_issues

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from pu_correction import wilson  # noqa: E402

DATASETS = HERE / "datasets"
WS_VERSION = "b04cdd5ac4b1"
CANON_OOF = DATASETS / "oof_ws_rebuild_canonical.parquet"
PRIOR_FLAGS = DATASETS / "cleanlab_mislabeled_negatives.parquet"
VERDICTS = DATASETS / "web_verify_verdicts_snapshot.parquet"
REAL = ("CONFIRMED", "LIKELY")


def high_conf_threshold(labels: np.ndarray, p: np.ndarray, target: float = 0.90) -> float:
    """Deepest score cut whose label=1 precision still clears `target`. Mirrors cleanlab_pass.py."""
    order = np.argsort(-p)
    precision = np.cumsum(labels[order]) / np.arange(1, len(p) + 1)
    ok = np.where(precision >= target)[0]
    assert ok.size, f"no threshold reaches {target:.0%} precision"
    return float(p[order][ok.max()])


def rate(mask: pd.Series, is_real: pd.Series) -> str:
    n = int(mask.sum())
    if n == 0:
        return "n=0"
    k = int(is_real[mask].sum())
    lo, hi = wilson(k, n)
    return f"{k}/{n} = {k / n:.3f}  [{lo:.2f}, {hi:.2f}]"


def main(ledger: bool) -> None:
    oof = pd.read_parquet(CANON_OOF)
    labels = oof["corroborated"].astype(int).to_numpy()
    p = oof["oof_hgb"].to_numpy(dtype=float)

    issues = find_label_issues(
        labels=labels, pred_probs=np.column_stack([1 - p, p]), filter_by="prune_by_noise_rate"
    )
    thr = high_conf_threshold(labels, p)
    oof["cleanlab_issue"] = issues
    oof["high_conf"] = (oof["oof_hgb"] >= thr) & (oof["corroborated"] == 0)

    neg = oof[oof["corroborated"] == 0]
    print(f"ws {WS_VERSION}: {len(oof):,} rows, {len(neg):,} negatives")
    print(f"90%-precision threshold: {thr:.4f}")
    print(f"cleanlab flags among negatives: {int(neg['cleanlab_issue'].sum()):,}")
    print(f"high-confidence flags:          {int(neg['high_conf'].sum()):,}")

    prior = pd.read_parquet(PRIOR_FLAGS)
    old_hc = set(prior.loc[prior["high_conf"], "campaign_id"])
    new_hc = set(neg.loc[neg["high_conf"], "campaign_id"])
    both = old_hc & new_hc
    print(
        f"\nhigh-conf stability across the rebuild: {len(both)} shared, "
        f"{len(old_hc - new_hc)} dropped, {len(new_hc - old_hc)} added "
        f"(Jaccard {len(both) / len(old_hc | new_hc):.3f})"
    )

    # --- the honest test: unbiased design sample only ---
    ver = pd.read_parquet(VERDICTS)
    design = ver[ver["wave"].isin(("stratified", "mopup")) & ~ver["verdict"].eq("NOT_FOUND_BUDGET")].copy()
    design["is_real"] = design["verdict"].isin(REAL)
    d = design.merge(
        neg[["campaign_id", "cleanlab_issue", "high_conf", "oof_hgb"]],
        on="campaign_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_old", ""),
    )
    print(f"\n=== flag quality on the design sample (n={len(d)}, drawn independently of any flag) ===")
    print(f"p(real | cleanlab flagged)    : {rate(d['cleanlab_issue'], d['is_real'])}")
    print(f"p(real | not flagged)         : {rate(~d['cleanlab_issue'], d['is_real'])}")
    print(f"p(real | high-confidence)     : {rate(d['high_conf'], d['is_real'])}")
    print(f"p(real | not high-confidence) : {rate(~d['high_conf'], d['is_real'])}")

    lift_cl = d.loc[d["cleanlab_issue"], "is_real"].mean() - d.loc[~d["cleanlab_issue"], "is_real"].mean()
    lift_hc = d.loc[d["high_conf"], "is_real"].mean() - d.loc[~d["high_conf"], "is_real"].mean()
    print(f"\nseparation: cleanlab {lift_cl:+.3f}, high-confidence {lift_hc:+.3f}")
    print(
        "Both intervals are wide at this n. What the design sample can rule out is a large "
        "effect, not a modest one; the flags are a triage aid, not a label."
    )

    out = neg[["campaign_id", "user_id", "oof_hgb", "cleanlab_issue", "high_conf"]]
    out.to_parquet(DATASETS / "cleanlab_flags_b04cdd5ac4b1.parquet", index=False)
    print(f"\nsaved datasets/cleanlab_flags_b04cdd5ac4b1.parquet ({len(out):,} rows)")

    if ledger:
        harness.append_ledger(
            "cleanlab re-run on rebuilt working set + design-sample validation",
            "other",
            "hgb",
            {
                "n_negatives": int(len(neg)),
                "n_cleanlab_flags": int(neg["cleanlab_issue"].sum()),
                "n_high_conf": int(neg["high_conf"].sum()),
                "high_conf_threshold": round(thr, 4),
                "high_conf_jaccard_vs_prior": round(len(both) / len(old_hc | new_hc), 4),
                "p_real_flagged": round(float(d.loc[d["cleanlab_issue"], "is_real"].mean()), 4),
                "p_real_unflagged": round(float(d.loc[~d["cleanlab_issue"], "is_real"].mean()), 4),
                "n_design_sample": int(len(d)),
            },
            notes="DATA-2238. First test of the cleanlab flags against ground truth. Scored ONLY "
            "on the stratified design sample: the sweep wave verified the old high-conf set "
            "itself, so scoring against it would ask whether the flags agree with their own "
            "selection.",
            area="area1",
            ver=WS_VERSION,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", action="store_true")
    main(ap.parse_args().ledger)
