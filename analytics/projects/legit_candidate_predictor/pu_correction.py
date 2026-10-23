"""DATA-2238 Area 1: PU correction of the corroboration label using the web-verification sample.

The label is `corroborated` (BR/TS/DDHQ presence on a completed election), but the quantity the
score is read as is `real candidate`. Positives are trustworthy; negatives are a mixture of real
candidates the corroboration pipeline missed and genuine noise. That is the textbook PU setting,
and the 2026-08-07 stratified verification wave is a *probability sample* of the negative class,
so the mixture rate is estimable by design rather than by assumption.

Estimator, and why not plain Elkan-Noto: Elkan-Noto assumes SCAR, one constant
c = p(labeled | real). The stratified wave measured p(real | label=0) per score bin and the curve
is NOT monotone in the score (0.52 / 0.33 / 0.43 / 0.47 / 0.77), so no single c can be right. What
the data does support is a two-group split, top score bin vs the rest, which is the only ordering
that survives its own confidence intervals. This module estimates that split by Horvitz-Thompson
weighting on the sampling design, then applies the per-row weights to correct base rate, precision,
recall, and calibration.

Frames are kept apart on purpose. Only the `stratified`+`mopup` wave is a probability sample of the
negative class; the `sweep` and `pilot` waves were adversarially selected (high-confidence cleanlab
flags), so they describe their slice and are reported separately. Pooling all 783 verdicts into one
rate is a composition artifact.

NOT_FOUND is evidence-shaped but ambiguous: absence of a web trace is weak evidence of not running,
not proof. Every estimate is therefore reported as a band. The floor counts NOT_FOUND as not-real;
the ceiling drops it and conditions on decided rows only. NOT_FOUND_BUDGET (the agent was blocked,
e.g. a county site was down) is always dropped: it is a failure of access, not an observation.

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/pu_correction.py --ledger
"""

import argparse
import sys
from math import sqrt
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402

DATASETS = HERE / "datasets"

# DATA-2312 canonical vintage. harness.WORKING_SET still points at the v1 file, so every path
# here is explicit; nothing in this module reads the harness default.
WS_FILE = DATASETS / "working_set_v2.parquet"
WS_VERSION = "b04cdd5ac4b1"
CANON_OOF = DATASETS / "oof_ws_rebuild_canonical.parquet"
CANON_MODEL_ID = "c1f1d210c9a5"

# The sampling frame was drawn on the sentinel-fix score vintage, not on either working set.
# Inclusion probabilities belong to THAT frame, so the design weights are computed against it.
FRAME_OOF = DATASETS / "oof_sentinel_fix.parquet"

VERDICTS = DATASETS / "web_verify_verdicts_snapshot.parquet"
DESIGN_WAVES = ("stratified", "mopup")
REAL = ("CONFIRMED", "LIKELY")
BIN_EDGES = [0.0, 0.2, 0.4, 0.6, 0.8, 1.001]
BIN_LABELS = ["b0_02", "b1_24", "b2_46", "b3_68", "b4_81"]
TOP_BIN = "b4_81"


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """Wilson score interval. Preferred over normal-approximation at these sample sizes."""
    if n == 0:
        return (np.nan, np.nan)
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return (max(0.0, centre - half), min(1.0, centre + half))


def score_bin(p: pd.Series) -> pd.Series:
    return pd.cut(p, BIN_EDGES, right=False, labels=BIN_LABELS)


def load_design_sample() -> pd.DataFrame:
    """The 150 verified rows that form a probability sample of the negative class."""
    v = pd.read_parquet(VERDICTS)
    s = v[v["wave"].isin(DESIGN_WAVES)].copy()
    assert s["score_bin"].notna().all(), "design rows must carry their stratum"
    assert s["bin_population"].notna().all(), "design rows must carry their stratum population"
    s["is_real"] = s["verdict"].isin(REAL)
    s["is_blocked"] = s["verdict"].eq("NOT_FOUND_BUDGET")
    s["is_traceless"] = s["verdict"].eq("NOT_FOUND")
    return s


def stratum_table(s: pd.DataFrame) -> pd.DataFrame:
    """Per-stratum p(real | label=0) under both NOT_FOUND readings, with design weights.

    floor  : NOT_FOUND counted as not-real (blocked rows dropped)
    ceiling: conditioned on decided rows only (NOT_FOUND and blocked both dropped)
    """
    rows = []
    for b, g in s.groupby("score_bin", observed=True):
        pop = int(g["bin_population"].iloc[0])
        real = int(g["is_real"].sum())
        contra = int((g["verdict"] == "CONTRADICTED").sum())
        n_floor = int((~g["is_blocked"]).sum())
        n_ceiling = real + contra
        f_lo, f_hi = wilson(real, n_floor)
        c_lo, c_hi = wilson(real, n_ceiling)
        rows.append(
            {
                "bin": b,
                "N": pop,
                "n_sampled": len(g),
                "real": real,
                "contradicted": contra,
                "traceless": int(g["is_traceless"].sum()),
                "blocked": int(g["is_blocked"].sum()),
                "p_floor": real / n_floor,
                "floor_lo": f_lo,
                "floor_hi": f_hi,
                "p_ceiling": real / n_ceiling,
                "ceil_lo": c_lo,
                "ceil_hi": c_hi,
                # Horvitz-Thompson: one sampled decided row stands for this many population rows.
                "ht_weight": pop / n_floor,
            }
        )
    return pd.DataFrame(rows).set_index("bin").reindex(BIN_LABELS).dropna(how="all")


def two_group_estimate(s: pd.DataFrame) -> pd.DataFrame:
    """Top score bin vs the rest: the only split the sample sizes actually support."""
    grp = np.where(s["score_bin"].eq(TOP_BIN), "top_bin", "rest")
    rows = []
    for name in ("rest", "top_bin"):
        g = s[grp == name]
        pop = int(g.groupby("score_bin", observed=True)["bin_population"].first().sum())
        real = int(g["is_real"].sum())
        contra = int((g["verdict"] == "CONTRADICTED").sum())
        n_floor = int((~g["is_blocked"]).sum())
        f_lo, f_hi = wilson(real, n_floor)
        c_lo, c_hi = wilson(real, real + contra)
        rows.append(
            {
                "group": name,
                "N": pop,
                "n_decided": n_floor,
                "real": real,
                "p_floor": real / n_floor,
                "floor_ci": f"[{f_lo:.2f}, {f_hi:.2f}]",
                "p_ceiling": real / (real + contra),
                "ceil_ci": f"[{c_lo:.2f}, {c_hi:.2f}]",
            }
        )
    return pd.DataFrame(rows).set_index("group")


def attach_weights(neg: pd.DataFrame, table: pd.DataFrame, two: pd.DataFrame) -> pd.DataFrame:
    """Give every negative row in the current working set its p(real | label=0) estimate.

    Keyed on the frame's score bin, not the current one: the weight is a property of the stratum
    the row would have been sampled from. Rows that post-date the frame (added by the DATA-2312
    rebuild) have no stratum and fall back to the group mean, which is flagged so the share of
    fallback rows stays visible.
    """
    out = neg.copy()
    grp = np.where(out["frame_bin"].eq(TOP_BIN), "top_bin", "rest")
    out["pu_group"] = np.where(out["frame_bin"].isna(), "unframed", grp)
    for col, src in (("w_floor", "p_floor"), ("w_ceiling", "p_ceiling")):
        w = pd.Series(np.nan, index=out.index)
        w[out["pu_group"] == "top_bin"] = two.loc["top_bin", src]
        w[out["pu_group"] == "rest"] = two.loc["rest", src]
        # Unframed rows get the population-weighted mean of the two groups.
        pooled = (two["N"] * two[src]).sum() / two["N"].sum()
        w[out["pu_group"] == "unframed"] = pooled
        out[col] = w
    # Per-bin weights kept alongside, for the finer-grained (wider-CI) reading.
    for col, src in (("w_bin_floor", "p_floor"), ("w_bin_ceiling", "p_ceiling")):
        out[col] = out["frame_bin"].map(table[src]).astype(float)
        out[col] = out[col].fillna(out["w_floor" if "floor" in col else "w_ceiling"])
    return out


def corrected_frontier(y: np.ndarray, p: np.ndarray, w: np.ndarray, thresholds: list[float]) -> pd.DataFrame:
    """Observed vs PU-corrected precision/recall at a set of score thresholds.

    Observed treats every label=0 as not-real. Corrected credits each label=0 row with `w`
    expected realness, so the numerator gains the corroboration misses the label never saw.
    """
    expected_real_total = y.sum() + w[y == 0].sum()
    rows = []
    for t in thresholds:
        sel = p >= t
        n_sel = int(sel.sum())
        if n_sel == 0:
            continue
        tp_obs = int(y[sel].sum())
        tp_corr = tp_obs + w[sel & (y == 0)].sum()
        rows.append(
            {
                "threshold": t,
                "n_selected": n_sel,
                "share_selected": n_sel / len(y),
                "precision_observed": tp_obs / n_sel,
                "precision_corrected": tp_corr / n_sel,
                "recall_observed": tp_obs / y.sum(),
                "recall_corrected": tp_corr / expected_real_total,
            }
        )
    return pd.DataFrame(rows)


def corrected_calibration(y: np.ndarray, p: np.ndarray, w: np.ndarray, bins: int = 10):
    """Score decile: what the score says, what corroboration shows, what realness implies."""
    q = pd.qcut(p, bins, duplicates="drop")
    df = pd.DataFrame({"q": q, "y": y, "p": p, "w": w})
    g = df.groupby("q", observed=True)
    return pd.DataFrame(
        {
            "n": g.size(),
            "score_mean": g["p"].mean(),
            "obs_corroborated": g["y"].mean(),
            "implied_real": g.apply(
                lambda s: (s["y"].sum() + s.loc[s["y"] == 0, "w"].sum()) / len(s), include_groups=False
            ),
        }
    ).round(3)


def slice_precision(v: pd.DataFrame, wave: str) -> dict:
    """Descriptive read of a non-probability wave. Its own slice only, never the class."""
    g = v[v["wave"] == wave]
    decided = g[~g["verdict"].eq("NOT_FOUND_BUDGET")]
    real = int(decided["verdict"].isin(REAL).sum())
    lo, hi = wilson(real, len(decided))
    return {
        "wave": wave,
        "n": len(g),
        "n_decided": len(decided),
        "real": real,
        "p_real_floor": round(real / len(decided), 4),
        "ci": f"[{lo:.2f}, {hi:.2f}]",
    }


def main(ledger: bool) -> None:
    s = load_design_sample()
    print(f"design sample: {len(s)} rows, waves {sorted(s.wave.unique())}\n")

    table = stratum_table(s)
    print("=== p(real | label=0) by sampling stratum (design sample, n=150) ===")
    show = table[
        [
            "N",
            "n_sampled",
            "real",
            "contradicted",
            "traceless",
            "blocked",
            "p_floor",
            "floor_lo",
            "floor_hi",
            "p_ceiling",
        ]
    ]
    print(show.round(3).to_string())
    print(
        "\nEvery per-bin interval is about +/-0.17 wide, so the bin-level shape is not "
        "resolvable at n=30/bin. The two-group split below is what the sample supports."
    )

    two = two_group_estimate(s)
    print("\n=== two-group PU estimate (top score bin vs the rest) ===")
    print(two.round(3).to_string())
    sep_lo = two.loc["top_bin", "p_floor"] - two.loc["rest", "p_floor"]
    print(f"\nseparation (floor): {sep_lo:+.3f}")

    # --- population-level corrected quantities on the canonical working set ---
    ws = pd.read_parquet(WS_FILE)
    assert harness.ws_version(WS_FILE) == WS_VERSION, "working set is not the DATA-2312 canonical"
    oof = pd.read_parquet(CANON_OOF)
    frame = pd.read_parquet(FRAME_OOF)[["campaign_id", "oof_hgb"]].rename(columns={"oof_hgb": "frame_score"})

    d = oof.merge(frame, on="campaign_id", how="left", validate="one_to_one")
    d["frame_bin"] = score_bin(d["frame_score"])
    assert len(d) == len(oof)

    neg = d[d["corroborated"] == 0].copy()
    neg = attach_weights(neg, table, two)
    unframed = (neg["pu_group"] == "unframed").mean()
    print(
        f"\nnegative rows: {len(neg):,}  without a sampling stratum (post-frame arrivals): "
        f"{unframed:.1%} (given the pooled weight)"
    )

    w_all = pd.Series(0.0, index=d.index)
    w_all.loc[neg.index] = neg["w_floor"]
    w_ceiling = pd.Series(0.0, index=d.index)
    w_ceiling.loc[neg.index] = neg["w_ceiling"]

    y = d["corroborated"].astype(int).to_numpy()
    p = d["oof_hgb"].to_numpy()

    base = y.mean()
    for name, w in (("floor", w_all.to_numpy()), ("ceiling", w_ceiling.to_numpy())):
        p_real = base + (1 - base) * w[y == 0].mean()
        c = base / p_real
        print(f"\n=== corrected population quantities ({name}) ===")
        print(f"observed corroboration rate : {base:.4f}")
        print(f"implied p(real)             : {p_real:.4f}")
        print(
            f"label frequency c=p(y=1|real): {c:.4f}   (corroboration misses "
            f"{1 - c:.1%} of real candidates)"
        )

    print("\n=== observed vs PU-corrected frontier (canonical OOF, floor weights) ===")
    fr = corrected_frontier(y, p, w_all.to_numpy(), [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9])
    print(fr.round(3).to_string(index=False))
    print(
        "\nRead: at any threshold, observed precision UNDERSTATES precision on realness, because "
        "some of the label=0 rows above the line are corroboration misses. Corrected recall is "
        "LOWER than observed for the same reason: the denominator of real candidates is bigger."
    )

    print("\n=== calibration: what the score means under each label (floor weights) ===")
    print(corrected_calibration(y, p, w_all.to_numpy()).to_string())

    # Sensitivity: the two-group weight is flat across the bottom four bins by construction, so
    # the bottom decile's implied realness could be an artifact of the coarse split. Re-run with
    # per-bin weights, which are noisier but let the bottom bin speak for itself.
    w_bin = pd.Series(0.0, index=d.index)
    w_bin.loc[neg.index] = neg["w_bin_floor"]
    print("\n--- sensitivity: same table with per-bin weights (noisier, not split-imposed) ---")
    print(corrected_calibration(y, p, w_bin.to_numpy())[["implied_real"]].to_string())

    # Where the misses live. If the design sample's real-but-uncorroborated rows concentrate in
    # the rule-decided bucket, the PU gap is the known coverage hole rather than matcher error.
    cov = ws[["campaign_id", "br_normalized_position_type"]].copy()
    cov["rule_decided"] = cov["br_normalized_position_type"].isna() | cov[
        "br_normalized_position_type"
    ].astype(str).eq("missing")
    ds = s.merge(cov, on="campaign_id", how="left", validate="one_to_one")
    ds = ds[~ds["is_blocked"]]
    print("\n=== where the corroboration misses sit (design sample, decided rows) ===")
    print(
        ds.groupby("rule_decided")
        .agg(
            n=("is_real", "size"),
            real=("is_real", "sum"),
            p_real=("is_real", "mean"),
        )
        .round(3)
        .to_string()
    )
    print("rule_decided=True is the no-BR-position bucket a rule routes, not the model.")

    # --- corrected evaluation set: model error vs label error on verified rows ---
    ver = pd.read_parquet(VERDICTS)
    ev = ver[ver["wave"].isin(DESIGN_WAVES)].merge(
        d[["campaign_id", "oof_hgb", "corroborated"]],
        on="campaign_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_frame", ""),
    )
    ev = ev[~ev["verdict"].eq("NOT_FOUND_BUDGET")]
    assert (ev["corroborated"] == 0).all(), "design sample must be negatives in the current ws"
    ev_real = ev["verdict"].isin(REAL)
    print(f"\n=== corrected eval set ({len(ev)} verified negatives, canonical score) ===")
    print(f"mean score, verified REAL     : {ev.loc[ev_real, 'oof_hgb'].mean():.3f}")
    print(f"mean score, verified NOT real : {ev.loc[~ev_real, 'oof_hgb'].mean():.3f}")
    from sklearn.metrics import roc_auc_score

    auc_within = roc_auc_score(ev_real.astype(int), ev["oof_hgb"])
    print(f"AUC separating real from not-real INSIDE the negative class: {auc_within:.4f}")
    print(
        "An AUC near 0.5 means the score does not rank realness inside label=0; it ranks "
        "corroboration. That is the finding the band design has to live with."
    )

    print("\n=== non-probability waves (their own slice, NOT the class) ===")
    for wave in ("sweep", "pilot"):
        print(slice_precision(ver, wave))

    out = neg[
        [
            "campaign_id",
            "user_id",
            "frame_score",
            "frame_bin",
            "pu_group",
            "w_floor",
            "w_ceiling",
            "w_bin_floor",
            "w_bin_ceiling",
        ]
    ]
    out.to_parquet(DATASETS / "pu_weights.parquet", index=False)
    print(f"\nsaved datasets/pu_weights.parquet ({len(out):,} negative rows)")

    if ledger:
        p_real_floor = base + (1 - base) * w_all.to_numpy()[y == 0].mean()
        p_real_ceiling = base + (1 - base) * w_ceiling.to_numpy()[y == 0].mean()
        harness.append_ledger(
            "PU correction v1 (two-group, design-weighted)",
            "other",
            "hgb",
            {
                "observed_base_rate": round(float(base), 4),
                "p_real_floor": round(float(p_real_floor), 4),
                "p_real_ceiling": round(float(p_real_ceiling), 4),
                "c_floor": round(float(base / p_real_floor), 4),
                "c_ceiling": round(float(base / p_real_ceiling), 4),
                "w_rest_floor": round(float(two.loc["rest", "p_floor"]), 4),
                "w_top_floor": round(float(two.loc["top_bin", "p_floor"]), 4),
                "auc_real_within_negatives": round(float(auc_within), 4),
                "n_design_sample": int(len(s)),
            },
            notes="DATA-2238. Design-based PU estimate from the stratified verification wave "
            "(the only probability sample of the negative class); sweep/pilot excluded as "
            "adversarially selected. Two groups, not five bins: per-bin CIs are +/-0.17. "
            "floor counts NOT_FOUND as not-real, ceiling conditions on decided rows. "
            "Split 'other' because this scores the LABEL, not a model.",
            area="area1",
            ver=WS_VERSION,
            model_id=CANON_MODEL_ID,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", action="store_true")
    main(ap.parse_args().ledger)
