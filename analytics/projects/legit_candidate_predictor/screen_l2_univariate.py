"""DATA-2239 L2/haystaq FULL-BLOCK screen, step 5: univariate map + correlation clusters.

Two deliverables, kept separate because they answer different questions:

1. UNIVARIATE INFORMATIVENESS, fold-internal (label used, discipline kept): per column,
   computed on the TRAINING side of each of the 5 frozen folds and averaged — never on
   full-data label rates (selection statistics from training folds only, Tristan
   2026-08-13; same contamination rule as the name-plausibility family).
     numeric columns      Mann-Whitney AUC vs corroborated (non-null rows)
     categorical columns  Cramér's V vs corroborated (levels with n >= 50)
   Reported with fold std so a "signal" that flips across folds reads as noise.

2. CORRELATION CLUSTERS, label-free: pairwise |Spearman| for numerics and Cramér's V for
   categoricals on the matched working-set rows, average-linkage clustered, cut at
   association >= 0.70. Any kept subset takes only the strongest representative (by the
   fold-internal univariate strength) of each cluster. The map is the asset; nothing is
   auto-dropped here.

Time-discipline notes ride along from l2_block_candidates.csv (apriori / year_gated /
date_gated per column).

Run (after build_l2_block.py):
  cd analytics && uv run --with scikit-learn --with pyarrow python \
      projects/legit_candidate_predictor/screen_l2_univariate.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform
from scipy.stats import rankdata

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402

MIN_LEVEL_N = 50
CLUSTER_AT = 0.70


def mw_auc(x: np.ndarray, y: np.ndarray) -> float:
    """Mann-Whitney AUC of x vs binary y (x non-null)."""
    n1, n0 = int(y.sum()), int((~y.astype(bool)).sum())
    if n1 == 0 or n0 == 0:
        return np.nan
    r = rankdata(x)
    return float((r[y.astype(bool)].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


def cramers_v(a: np.ndarray, b: np.ndarray, min_n: int = 0) -> float:
    """Cramér's V between two integer-coded arrays (>=0; -1 = missing excluded)."""
    m = (a >= 0) & (b >= 0)
    a, b = a[m], b[m]
    if len(a) < 100:
        return np.nan
    ka, kb = a.max() + 1, b.max() + 1
    ct = np.bincount(a * kb + b, minlength=ka * kb).reshape(ka, kb).astype(float)
    if min_n:
        keep = ct.sum(axis=1) >= min_n
        ct = ct[keep]
        if ct.shape[0] < 2:
            return np.nan
    n = ct.sum()
    if n == 0 or ct.shape[0] < 2 or ct.shape[1] < 2:
        return np.nan
    exp = np.outer(ct.sum(axis=1), ct.sum(axis=0)) / n
    with np.errstate(divide="ignore", invalid="ignore"):
        chi2 = np.nansum(np.where(exp > 0, (ct - exp) ** 2 / exp, 0.0))
    r, k = ct.shape
    denom = n * (min(r - 1, k - 1))
    return float(np.sqrt(chi2 / denom)) if denom > 0 else np.nan


def cluster(assoc: pd.DataFrame, strength: pd.Series) -> pd.DataFrame:
    """Average-linkage clusters from an association matrix; representative = strongest."""
    a = assoc.fillna(0.0).clip(0, 1)
    np.fill_diagonal(a.values, 1.0)
    d = squareform((1 - a).values, checks=False)
    labels = fcluster(linkage(d, method="average"), t=1 - CLUSTER_AT, criterion="distance")
    out = pd.DataFrame({"column": a.index, "cluster": labels})
    out["strength"] = out.column.map(strength)
    out["representative"] = out.groupby("cluster")["strength"].transform(
        lambda s: s.idxmax() if s.notna().any() else s.index[0]
    )
    # representative currently holds the row index of the max; map to the column name
    out["representative"] = out["representative"].map(out["column"])
    return out.sort_values(["cluster", "strength"], ascending=[True, False])


if __name__ == "__main__":
    ws = pd.read_parquet(harness.WORKING_SET)
    folds = harness.load_folds()
    ws = ws.merge(folds[["campaign_id", "fold"]], on="campaign_id", how="left")
    fam = pd.read_parquet(HERE / "datasets" / "l2_block_features.parquet")
    df = ws[["user_id", "campaign_id", "corroborated", "fold"]].merge(
        fam, on="user_id", how="left", validate="many_to_one"
    )
    y_all = df["corroborated"].astype(int).to_numpy()

    cand = pd.read_csv(HERE / "datasets" / "l2_block_candidates.csv")
    tdisc = dict(zip("l2b_" + cand.column_name, cand.time_discipline, strict=False))

    cols = [c for c in fam.columns if c != "user_id"]
    num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in cols if c not in num_cols]
    print(
        f"{len(cols)} columns ({len(num_cols)} numeric, {len(cat_cols)} categorical) "
        f"on {len(df):,} ws rows; matched share "
        f"{df['l2b_matched'].fillna(0).mean():.1%}"
    )

    # integer-code categoricals once (missing = -1)
    codes = {c: pd.factorize(df[c], use_na_sentinel=True)[0] for c in cat_cols}

    rows = []
    fold_ids = sorted(df["fold"].dropna().unique())
    for c in cols:
        per_fold = []
        if c in num_cols:
            x_all = df[c].to_numpy(dtype=float)
            for k in fold_ids:
                tr = (df["fold"] != k).to_numpy()
                m = tr & ~np.isnan(x_all)
                per_fold.append(mw_auc(x_all[m], y_all[m]) if m.sum() >= 200 else np.nan)
            kind, base = "auc", 0.5
        else:
            x_all = codes[c]
            for k in fold_ids:
                tr = (df["fold"] != k).to_numpy()
                per_fold.append(cramers_v(x_all[tr], np.where(tr, y_all, 0)[tr], min_n=MIN_LEVEL_N))
            kind, base = "cramers_v", 0.0
        pf = pd.Series(per_fold, dtype=float)
        strength = (pf - base).abs().mean() if pf.notna().any() else np.nan
        rows.append(
            {
                "column": c,
                "kind": kind,
                "mean": round(float(pf.mean()), 4) if pf.notna().any() else np.nan,
                "std": round(float(pf.std()), 4) if pf.notna().sum() > 1 else np.nan,
                "strength": round(float(strength), 4) if pd.notna(strength) else np.nan,
                "coverage_ws": round(float(df[c].notna().mean()), 4),
                "time_discipline": tdisc.get(c, "derived"),
            }
        )
    uni = pd.DataFrame(rows).sort_values("strength", ascending=False)
    uni.to_csv(HERE / "datasets" / "l2_block_univariate.csv", index=False)
    print("\ntop 25 by fold-internal univariate strength:")
    print(uni.head(25).to_string(index=False))

    # ---- label-free association matrices on matched rows only
    matched = df["l2b_matched"].fillna(0).astype(int).eq(1).to_numpy()
    strength_map = uni.set_index("column")["strength"]

    sub = df.loc[matched, num_cols].astype(float)
    keep_num = [c for c in num_cols if sub[c].notna().sum() >= 500 and sub[c].nunique(dropna=True) > 1]
    ranks = sub[keep_num].rank()
    corr = ranks.corr(min_periods=500).abs()
    ncl = cluster(corr, strength_map)
    ncl["kind"] = "numeric"

    keep_cat = [
        c
        for c in cat_cols
        if (codes[c][matched] >= 0).sum() >= 500
        and len(np.unique(codes[c][matched][codes[c][matched] >= 0])) > 1
    ]
    vmat = pd.DataFrame(np.eye(len(keep_cat)), index=keep_cat, columns=keep_cat)
    for i, a in enumerate(keep_cat):
        ca = codes[a][matched]
        for b in keep_cat[i + 1 :]:
            v = cramers_v(ca, codes[b][matched])
            vmat.loc[a, b] = vmat.loc[b, a] = v if pd.notna(v) else 0.0
    ccl = cluster(vmat, strength_map)
    ccl["kind"] = "categorical"

    cl = pd.concat([ncl, ccl], ignore_index=True)
    cl.to_csv(HERE / "datasets" / "l2_block_clusters.csv", index=False)
    multi = cl.groupby(["kind", "cluster"]).size()
    print(
        f"\nclusters at >= {CLUSTER_AT}: numeric {ncl.cluster.nunique()} "
        f"(from {len(keep_num)} cols), categorical {ccl.cluster.nunique()} "
        f"(from {len(keep_cat)}); non-singleton {int((multi > 1).sum())}"
    )
    big = cl[cl.groupby(["kind", "cluster"])["column"].transform("size") >= 4]
    print("\nlargest clusters (representative first):")
    for _, g in big.groupby(["kind", "cluster"]):
        print(
            f"  [{g.kind.iloc[0]}] rep={g.representative.iloc[0]}: "
            f"{', '.join(g.column.head(8))}{' ...' if len(g) > 8 else ''}"
        )
