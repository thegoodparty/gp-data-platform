"""DATA-2239 L2/haystaq FULL-BLOCK screen: promising-subset probes (Tristan, 2026-08-13).

The full-block run came back flat (-0.0046/-0.0064/-0.0018), but 791 columns carry a
measured dilution tax (dead-column tax was -0.002 at 25 columns on the easy-match
family), so flat-at-full-width does not by itself rule out a small orthogonal subset.
These probes give the most promising columns a clean shot WITHOUT selection leakage:

  A  top-K by individual lift          per fold, rank every block column on that fold's
                                       TRAINING side only; add the top K to the baseline.
  B  top-K orthogonal representatives  same ranking, but at most one column per
                                       label-free correlation cluster
                                       (datasets/l2_block_clusters.csv).
  C  top-K with a coverage floor       as A, restricted to columns filled for >= 20% of
                                       working-set rows (sparse leaders cannot move a
                                       pooled AUC).
  D  stacked block score               the block compressed to ONE column: the
                                       family-alone OOF prediction already produced on
                                       these folds by exp_family (standard OOF stacking;
                                       its slight optimism is conservative when the
                                       expected result is flat). Zero dilution — if this
                                       is flat, the block's information is provably
                                       inside the baseline.
  E  PCA components                    top 25 principal components of the standardized
                                       hs_ score block (fit per training fold,
                                       mean-imputed), added to the baseline. Catches
                                       signal smeared thinly across correlated columns.

Every variant also gets a MATCHED-ONLY read (AUC among the 56% of working-set rows the
bridge resolves, vs the baseline on the same rows): unmatched rows are NaN by
construction and drag pooled deltas toward zero.

Selection statistics are recomputed inside each fold's training side (and on the 2025
train side for the forward split) — never on full-data label rates. Ranking scale:
Gini = 2*|MW-AUC - 0.5| for numerics, Cramér's V for categoricals (both 0-1; close
enough for a shortlist probe, noted as approximate).

Run (after exp_family baseline artifacts exist):
  cd analytics && uv run --with scikit-learn --with pyarrow python \
      projects/legit_candidate_predictor/exp_l2_subsets.py [--k 25]
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from exp_family import BASELINE, HGB, frame, load  # noqa: E402
from mvp_model import engineer, recency_weights  # noqa: E402
from screen_l2_univariate import cramers_v, mw_auc  # noqa: E402
from sklearn.decomposition import PCA  # noqa: E402

N_PCA = 25

COVERAGE_FLOOR = 0.20


def strengths(
    df: pd.DataFrame, y: np.ndarray, mask: np.ndarray, cols: list[str], codes: dict[str, np.ndarray]
) -> pd.Series:
    """Per-column selection score on the masked (training) rows only."""
    out = {}
    for c in cols:
        if c in codes:
            out[c] = cramers_v(codes[c][mask], y[mask].astype(int), min_n=50)
        else:
            x = df[c].to_numpy(dtype=float)[mask]
            yy = y[mask]
            ok = ~np.isnan(x)
            out[c] = 2 * abs(mw_auc(x[ok], yy[ok]) - 0.5) if ok.sum() >= 200 else np.nan
    return pd.Series(out, dtype=float)


def pick(s: pd.Series, k: int, cluster_of: dict[str, tuple] | None = None) -> list[str]:
    s = s.dropna().sort_values(ascending=False)
    if cluster_of is None:
        return list(s.index[:k])
    chosen, used = [], set()
    for c in s.index:
        cl = cluster_of.get(c, ("solo", c))
        if cl in used:
            continue
        used.add(cl)
        chosen.append(c)
        if len(chosen) == k:
            break
    return chosen


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=25)
    ap.add_argument("--only", default="", help="comma list of variant letters to run, e.g. A,B (default all)")
    args = ap.parse_args()
    only = set(args.only.upper().replace(",", "")) if args.only else set("ABCDE")

    df, fam, cols = load("l2_block_features.parquet", "user_id")
    eng = engineer(df)
    y = eng["corroborated"].astype(int)
    y_np = y.to_numpy()
    fold = eng["fold"]
    w = recency_weights(eng)
    x = frame(df, eng, cols)

    codes = {
        c: pd.factorize(df[c], use_na_sentinel=True)[0]
        for c in cols
        if not pd.api.types.is_numeric_dtype(df[c])
    }
    coverage = df[cols].notna().mean()

    cl = pd.read_csv(HERE / "datasets" / "l2_block_clusters.csv")
    cluster_of = {r.column: (r.kind, r.cluster) for r in cl.itertuples()}

    base_cols = [c for c in x.columns if c not in cols]
    variants: dict[str, dict] = {
        "A_top_by_lift": dict(pool=cols, clusters=None),
        "B_cluster_reps": dict(pool=cols, clusters=cluster_of),
        "C_coverage_floor": dict(pool=[c for c in cols if coverage[c] >= COVERAGE_FLOOR], clusters=None),
    }
    print(
        f"pools: A/B {len(cols)}  C {len(variants['C_coverage_floor']['pool'])} "
        f"(coverage >= {COVERAGE_FLOOR:.0%});  K = {args.k}"
    )

    role = eng["forward_role"]
    tr_f, te_f = role.eq("train_2025").to_numpy(), role.eq("test_2026plus").to_numpy()

    matched = pd.to_numeric(df["l2b_matched"], errors="coerce").fillna(0).eq(1).to_numpy()
    base_oof = pd.read_parquet(HERE / "datasets" / "oof_predictions_recency240.parquet")
    base_oof = eng[["campaign_id"]].merge(base_oof[["campaign_id", "oof_hgb"]], on="campaign_id", how="left")[
        "oof_hgb"
    ]
    base_m = float(roc_auc_score(y[matched], base_oof[matched]))
    print(
        f"baseline matched-only OOF AUC: {base_m:.4f} "
        f"(n={int(matched.sum()):,}, base rate {y[matched].mean():.3f})"
    )
    blk = pd.read_parquet(HERE / "datasets" / "oof_l2_block_features.parquet")
    blk = eng[["campaign_id"]].merge(blk, on="campaign_id", how="left")
    fb_m = float(roc_auc_score(y[matched], blk.loc[matched, "oof_hgb"]))
    print(f"full-block matched-only OOF AUC: {fb_m:.4f} ({fb_m - base_m:+.4f})")

    def report(name: str, oof: np.ndarray, fwd: float, extra: str = "") -> None:
        metrics = harness.evaluate(y, oof, eng["decidable"])
        m_auc = float(roc_auc_score(y[matched], oof[matched]))
        print(f"\n=== {name} ===")
        print(
            f"pooled {metrics['roc_auc']:.4f} "
            f"({metrics['roc_auc'] - BASELINE['roc_auc']:+.4f})   "
            f"decidable {metrics['decidable_roc_auc']:.4f} "
            f"({metrics['decidable_roc_auc'] - BASELINE['decidable_roc_auc']:+.4f})   "
            f"forward {fwd:.4f} ({fwd - BASELINE['forward_roc_auc']:+.4f})   "
            f"matched-only {m_auc:.4f} ({m_auc - base_m:+.4f})"
        )
        if extra:
            print(extra)

    for name, v in variants.items():
        if name[0] not in only:
            continue
        oof = np.zeros(len(x))
        picks_by_fold = []
        for k in sorted(fold.unique()):
            tr = fold.ne(k).to_numpy()
            s = strengths(df, y_np, tr, v["pool"], codes)
            sel = pick(s, args.k, v["clusters"])
            picks_by_fold.append(sel)
            use = x[base_cols + sel]
            m = HistGradientBoostingClassifier(**HGB).fit(use[tr], y[tr], sample_weight=w[tr])
            oof[~tr] = m.predict_proba(use[~tr])[:, 1]

        s_f = strengths(df, y_np, tr_f, v["pool"], codes)
        sel_f = pick(s_f, args.k, v["clusters"])
        use = x[base_cols + sel_f]
        mf = HistGradientBoostingClassifier(**HGB).fit(use[tr_f], y[tr_f], sample_weight=w[tr_f])
        fwd = float(roc_auc_score(y[te_f], mf.predict_proba(use[te_f])[:, 1]))

        stable = set.intersection(*(set(p) for p in picks_by_fold))
        report(
            f"{name} (K={args.k})",
            oof,
            fwd,
            extra=f"picks stable across all 5 folds: {len(stable)}/{args.k}\n"
            "forward-split picks: " + ", ".join(c.replace("l2b_", "") for c in sel_f),
        )

    if "D" not in only:
        sys.exit(0)
    # ---- D: stacked block score (one column, zero dilution)
    x_d = x[base_cols].copy()
    x_d["l2b_block_score"] = blk["oof_family_only"].to_numpy()
    oof = np.zeros(len(x_d))
    for k in sorted(fold.unique()):
        tr = fold.ne(k).to_numpy()
        m = HistGradientBoostingClassifier(**HGB).fit(x_d[tr], y[tr], sample_weight=w[tr])
        oof[~tr] = m.predict_proba(x_d[~tr])[:, 1]
    # forward: block-only model fit on 2025 scores 2026; meta trained on OOF scores
    m_blk = HistGradientBoostingClassifier(**HGB).fit(x[cols][tr_f], y[tr_f], sample_weight=w[tr_f])
    x_df = x_d.copy()
    x_df.loc[te_f, "l2b_block_score"] = m_blk.predict_proba(x[cols][te_f])[:, 1]
    mf = HistGradientBoostingClassifier(**HGB).fit(x_df[tr_f], y[tr_f], sample_weight=w[tr_f])
    fwd = float(roc_auc_score(y[te_f], mf.predict_proba(x_df[te_f])[:, 1]))
    report(
        "D_stacked_block_score",
        oof,
        fwd,
        extra=f"block-score column alone (family-alone OOF AUC): "
        f"{roc_auc_score(y, blk['oof_family_only']):.4f}",
    )

    if "E" not in only:
        sys.exit(0)
    # ---- E: PCA of the hs_ score block, fold-internal
    hs_cols = [c for c in cols if c.startswith("l2b_hs_") and pd.api.types.is_numeric_dtype(df[c])]
    H = df[hs_cols].to_numpy(dtype=float)
    oof = np.zeros(len(x))
    pc_names = [f"l2b_pc{i + 1}" for i in range(N_PCA)]

    def pca_frame(train_mask: np.ndarray) -> pd.DataFrame:
        mu = np.nanmean(H[train_mask], axis=0)
        sd = np.nanstd(H[train_mask], axis=0)
        sd[sd == 0] = 1.0
        Z = (np.where(np.isnan(H), mu, H) - mu) / sd
        p = PCA(n_components=N_PCA, random_state=7).fit(Z[train_mask])
        out = x[base_cols].copy()
        out[pc_names] = p.transform(Z)
        return out

    for k in sorted(fold.unique()):
        tr = fold.ne(k).to_numpy()
        xp = pca_frame(tr)
        m = HistGradientBoostingClassifier(**HGB).fit(xp[tr], y[tr], sample_weight=w[tr])
        oof[~tr] = m.predict_proba(xp[~tr])[:, 1]
    xp = pca_frame(tr_f)
    mf = HistGradientBoostingClassifier(**HGB).fit(xp[tr_f], y[tr_f], sample_weight=w[tr_f])
    fwd = float(roc_auc_score(y[te_f], mf.predict_proba(xp[te_f])[:, 1]))
    report(
        f"E_pca_{N_PCA}_hs_components",
        oof,
        fwd,
        extra=f"components from {len(hs_cols)} hs_ columns, " "mean-imputed + standardized per training fold",
    )
