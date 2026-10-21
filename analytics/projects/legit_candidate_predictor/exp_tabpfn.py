"""DATA-2225 model-family experiment: TabPFN v2 as a ceiling probe.

Same frozen folds, same working set, EXACTLY CAT_COLS + NUM_COLS from mvp_model
(point-in-time allowlist). CAT_COLS ordinally encoded per train fold (unseen ->
reserved code); TabPFN handles numerics/NaN natively. Train folds subsampled to
10,000 rows (stratified, random_state=7) to respect TabPFN's pretraining limit
and the CPU budget.

Usage (from analytics/):
  uv run --with scikit-learn --with pyarrow --with tabpfn python \
      projects/legit_candidate_predictor/exp_tabpfn.py fold 0
  ... exp_tabpfn.py forward
  ... exp_tabpfn.py combine

Per-fold predictions land in the session scratchpad; `combine` pools them,
prints metrics, and writes datasets/oof_tabpfn.parquet. Does NOT touch the
results ledger.
"""

import argparse
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import harness  # noqa: E402
from mvp_model import CAT_COLS, NUM_COLS, engineer  # noqa: E402

# Per-fold outputs, so a crashed multi-hour run resumes instead of restarting.
SCRATCH = harness.DATASETS / "tabpfn_scratch"
SCRATCH.mkdir(parents=True, exist_ok=True)
SUBSAMPLE = 10_000
SEED = 7
N_ESTIMATORS = 4  # TabPFN v2 default


def load() -> pd.DataFrame:
    df = engineer(pd.read_parquet(HERE / "datasets" / "working_set.parquet"))
    folds = harness.load_folds()
    df = df.merge(
        folds[["campaign_id", "fold", "forward_role", "decidable"]],
        on="campaign_id",
        how="left",
        validate="one_to_one",
    )
    assert df["fold"].notna().all(), "fold merge left gaps"
    return df


def subsample_train(tr: pd.DataFrame) -> pd.DataFrame:
    if len(tr) <= SUBSAMPLE:
        return tr
    keep, _ = train_test_split(tr, train_size=SUBSAMPLE, stratify=tr["corroborated"], random_state=SEED)
    return keep


def encode(tr: pd.DataFrame, te: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Ordinal-encode CAT_COLS fit on train only; unseen categories -> reserved code."""
    xtr = tr[CAT_COLS + NUM_COLS].copy()
    xte = te[CAT_COLS + NUM_COLS].copy()
    for c in CAT_COLS:
        mapping = {v: i for i, v in enumerate(sorted(xtr[c].unique()))}
        reserved = len(mapping)
        xtr[c] = xtr[c].map(mapping).astype(float)
        xte[c] = xte[c].map(mapping).fillna(reserved).astype(float)
    return xtr.to_numpy(dtype=np.float64), xte.to_numpy(dtype=np.float64)


PREDICT_BATCH = 512  # single-shot predict on 4k rows was OOM-killed on CPU


def fit_predict(xtr, ytr, xte) -> np.ndarray:
    import torch
    from tabpfn import TabPFNClassifier

    torch.set_num_threads(max(1, (os.cpu_count() or 4) - 1))
    clf = TabPFNClassifier(
        device="cpu",
        n_estimators=N_ESTIMATORS,
        categorical_features_indices=list(range(len(CAT_COLS))),
        random_state=SEED,
        # train stays at <=10k rows (the pretraining limit); this flag also lifts
        # the >1000-sample CPU refusal, which is the only reason it's set
        ignore_pretraining_limits=True,
    )
    clf.fit(xtr, ytr)
    out = np.empty(len(xte))
    t0 = time.time()
    for lo in range(0, len(xte), PREDICT_BATCH):
        hi = min(lo + PREDICT_BATCH, len(xte))
        out[lo:hi] = clf.predict_proba(xte[lo:hi])[:, 1]
        print(f"  predicted {hi}/{len(xte)}  ({time.time() - t0:.0f}s elapsed)", flush=True)
    return out


def run_fold(fold: int) -> None:
    df = load()
    tr = subsample_train(df[df["fold"] != fold])
    te = df[df["fold"] == fold]
    print(
        f"fold {fold}: train {len(tr):,} (subsampled from {(df['fold'] != fold).sum():,}), "
        f"test {len(te):,}",
        flush=True,
    )
    xtr, xte = encode(tr, te)
    t0 = time.time()
    p = fit_predict(xtr, tr["corroborated"].astype(int).to_numpy(), xte)
    dt = time.time() - t0
    y = te["corroborated"].astype(int)
    print(
        f"fold {fold}: {dt / 60:.1f} min  roc_auc={roc_auc_score(y, p):.4f}  "
        f"pr_auc={average_precision_score(y, p):.4f}",
        flush=True,
    )
    out = te[["campaign_id", "user_id", "corroborated", "fold", "decidable"]].copy()
    out["p_tabpfn"] = p
    out["split"] = "oof"
    out.to_parquet(SCRATCH / f"tabpfn_fold{fold}.parquet", index=False)


def run_forward() -> None:
    df = load()
    tr_all = df[df["forward_role"] == "train_2025"]
    te = df[df["forward_role"] == "test_2026plus"]
    tr = subsample_train(tr_all)
    print(f"forward: train {len(tr):,} (from {len(tr_all):,} 2025 rows), test {len(te):,}", flush=True)
    xtr, xte = encode(tr, te)
    t0 = time.time()
    p = fit_predict(xtr, tr["corroborated"].astype(int).to_numpy(), xte)
    dt = time.time() - t0
    y = te["corroborated"].astype(int)
    print(
        f"forward: {dt / 60:.1f} min  roc_auc={roc_auc_score(y, p):.4f}  "
        f"pr_auc={average_precision_score(y, p):.4f}",
        flush=True,
    )
    out = te[["campaign_id", "user_id", "corroborated", "fold", "decidable"]].copy()
    out["p_tabpfn"] = p
    out["split"] = "forward"
    out.to_parquet(SCRATCH / "tabpfn_forward.parquet", index=False)


def combine() -> None:
    parts = sorted(SCRATCH.glob("tabpfn_fold*.parquet"))
    if not parts:
        raise SystemExit("no fold outputs found")
    oof = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    folds_done = sorted(oof["fold"].unique().tolist())
    y = oof["corroborated"].astype(int)
    p = oof["p_tabpfn"].to_numpy()
    d = oof["decidable"].astype(bool)
    print(f"folds included: {folds_done}  n={len(oof):,}  pos={y.mean():.4f}")
    print(f"pooled roc_auc={roc_auc_score(y, p):.4f}  pr_auc={average_precision_score(y, p):.4f}")
    print(
        f"decidable (n={d.sum():,}) roc_auc={roc_auc_score(y[d], p[d]):.4f}  "
        f"pr_auc={average_precision_score(y[d], p[d]):.4f}"
    )
    print("\nper-fold:")
    for f, g in oof.groupby("fold"):
        print(f"  fold {f}: n={len(g):,}  roc_auc=" f"{roc_auc_score(g['corroborated'], g['p_tabpfn']):.4f}")

    frames = [oof]
    fwd_path = SCRATCH / "tabpfn_forward.parquet"
    if fwd_path.exists():
        fwd = pd.read_parquet(fwd_path)
        yf, pf = fwd["corroborated"].astype(int), fwd["p_tabpfn"].to_numpy()
        print(
            f"\nforward (n={len(fwd):,}) roc_auc={roc_auc_score(yf, pf):.4f}  "
            f"pr_auc={average_precision_score(yf, pf):.4f}"
        )
        frames.append(fwd)
    pd.concat(frames, ignore_index=True).to_parquet(HERE / "datasets" / "oof_tabpfn.parquet", index=False)
    print("\nsaved -> datasets/oof_tabpfn.parquet")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    pf = sub.add_parser("fold")
    pf.add_argument("fold", type=int, nargs="+")
    sub.add_parser("forward")
    sub.add_parser("combine")
    args = ap.parse_args()
    if args.cmd == "fold":
        for f in args.fold:
            if (SCRATCH / f"tabpfn_fold{f}.parquet").exists():
                print(f"fold {f}: already done, skipping", flush=True)
                continue
            run_fold(f)
    elif args.cmd == "forward":
        run_forward()
    else:
        combine()
