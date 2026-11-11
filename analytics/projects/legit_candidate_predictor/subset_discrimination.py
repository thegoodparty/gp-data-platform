"""DATA-2241 Area 4, item 2: activated-only and ICP-only discrimination reads.

The ticket's question: is separation easier or harder within the engaged population? Useful because
if the score works badly exactly where the product has the most signal, that is worth knowing before
anyone builds engagement-based features or reads score bands inside an engaged cohort.

The trap this script is built to avoid: **AUC is not comparable across subsets.** It moves with the
base rate and with n, so a subset scoring lower is not evidence that discrimination is harder there.
Two guards. Every AUC carries a bootstrap CI, and every row carries its n and base rate so the
reader can see when a gap is inside the noise. A subset with 763 rows will have an interval wide
enough to swallow most apparent differences, and that is the honest answer rather than a limitation.

Definitions come from the cached artifacts, not re-derived: `is_activated` and the engagement counts
from `activity.parquet` (joins to the rebuilt working set at 100% coverage), `is_pro` and
`icp_office_win` from the working set as the slice dimensions they were pulled in as. Note `is_pro`
exists on both frames; the working-set copy is used, since that is the one the population was
defined with.

Run (from analytics/):
  uv run --with scikit-learn --with pyarrow --with mlflow-skinny python \
      projects/legit_candidate_predictor/subset_discrimination.py --ledger
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402

DATASETS = HERE / "datasets"
WS_FILE = DATASETS / "working_set_v2.parquet"
WS_VERSION = "b04cdd5ac4b1"
CANON_OOF = DATASETS / "oof_ws_rebuild_canonical.parquet"
CANON_MODEL_ID = "c1f1d210c9a5"
ACTIVITY = DATASETS / "activity.parquet"
N_BOOT = 500
MIN_N = 150
SEED = 7


def auc_ci(y: np.ndarray, p: np.ndarray, n_boot: int = N_BOOT) -> tuple[float, float, float]:
    """AUC with a bootstrap percentile CI. Resamples rows, so it reflects subset size honestly."""
    if len(np.unique(y)) < 2:
        return (np.nan, np.nan, np.nan)
    point = float(roc_auc_score(y, p))
    rng = np.random.default_rng(SEED)
    boots = []
    for _ in range(n_boot):
        idx = rng.integers(0, len(y), len(y))
        if len(np.unique(y[idx])) < 2:
            continue
        boots.append(roc_auc_score(y[idx], p[idx]))
    if not boots:
        return (point, np.nan, np.nan)
    return (point, float(np.percentile(boots, 2.5)), float(np.percentile(boots, 97.5)))


def main(ledger: bool) -> None:
    assert harness.ws_version(WS_FILE) == WS_VERSION, "not the DATA-2312 canonical working set"
    ws = pd.read_parquet(WS_FILE)[["campaign_id", "user_id", "is_pro", "icp_office_win"]]
    act = pd.read_parquet(ACTIVITY)[
        ["user_id", "is_activated", "total_campaigns_sent", "dashboard_view_count"]
    ]
    oof = pd.read_parquet(CANON_OOF)

    df = oof.merge(ws, on=["campaign_id", "user_id"], how="left", validate="one_to_one")
    df = df.merge(act, on="user_id", how="left", validate="many_to_one")
    assert len(df) == len(oof), "join changed the row count"

    df["activated"] = df["is_activated"].astype("boolean").fillna(False)
    df["pro"] = df["is_pro"].astype("boolean").fillna(False)
    df["sender"] = df["total_campaigns_sent"].fillna(0) > 0
    df["viewer"] = df["dashboard_view_count"].fillna(0) > 0
    df["icp"] = df["icp_office_win"].astype("boolean")

    subsets = {
        "ALL": pd.Series(True, index=df.index),
        "activated": df["activated"],
        "not activated": ~df["activated"],
        "pro": df["pro"],
        "not pro": ~df["pro"],
        "sender": df["sender"],
        "not sender": ~df["sender"],
        "viewer": df["viewer"],
        "not viewer": ~df["viewer"],
        "icp office": df["icp"].fillna(False),
        "non-icp office": (~df["icp"].fillna(True)),
    }

    # Two subsets that are the same set produce identical rows and read as corroboration of each
    # other. Detect rather than silently duplicate: `is_activated` turns out to BE "sent a
    # campaign", which is worth knowing about the definition, not hiding in a table.
    keys = {k: tuple(np.flatnonzero(v.to_numpy(dtype=bool))) for k, v in subsets.items()}
    seen: dict[tuple, str] = {}
    for name, k in keys.items():
        if k in seen:
            print(
                f"NOTE: subset '{name}' is identical to '{seen[k]}' ({len(k)} rows); "
                f"they are one fact, not two"
            )
        else:
            seen[k] = name

    y_all = df["corroborated"].to_numpy()
    p_all = df["oof_hgb"].to_numpy()

    rows = []
    for name, mask in subsets.items():
        m = mask.to_numpy(dtype=bool)
        if m.sum() < MIN_N:
            rows.append({"subset": name, "n": int(m.sum()), "note": "below minimum"})
            continue
        point, lo, hi = auc_ci(y_all[m], p_all[m])
        rows.append(
            {
                "subset": name,
                "n": int(m.sum()),
                "share": m.mean(),
                "base_rate": float(y_all[m].mean()),
                "auc": point,
                "ci_lo": lo,
                "ci_hi": hi,
                "ci_width": hi - lo,
                "note": "",
            }
        )
    t = pd.DataFrame(rows)
    pd.set_option("display.width", 200)
    print(f"model {CANON_MODEL_ID} on ws {WS_VERSION}\n")
    print(f"=== discrimination within subsets (bootstrap CIs, {N_BOOT} resamples) ===")
    print(t.round(4).to_string(index=False))

    base = t.loc[t["subset"] == "ALL"].iloc[0]
    print(f"\npooled AUC {base['auc']:.4f} [{base['ci_lo']:.4f}, {base['ci_hi']:.4f}]")
    print(
        "\nRead each pair against its own intervals, not against the pooled number. Where a "
        "subset's CI contains the pooled AUC, there is no evidence discrimination differs there."
    )

    scored = t[t["note"].eq("")]
    overlaps = scored[(scored["ci_lo"] <= base["auc"]) & (scored["ci_hi"] >= base["auc"])]
    print(f"\nsubsets whose CI contains the pooled AUC: " f"{sorted(set(overlaps['subset']) - {'ALL'})}")
    distinct = scored[(scored["ci_hi"] < base["auc"]) | (scored["ci_lo"] > base["auc"])]
    print(f"subsets that genuinely differ from pooled: " f"{sorted(set(distinct['subset']) - {'ALL'})}")

    # If BOTH halves of a partition sit below the pooled figure, the pooled figure is partly
    # measuring between-group separation rather than within-group ranking. That is not a paradox,
    # it is what makes pooled AUC a poor summary here, and it is worth saying out loud.
    for a, b, axis in (
        ("icp office", "non-icp office", "ICP"),
        ("viewer", "not viewer", "dashboard viewing"),
        ("activated", "not activated", "activation"),
    ):
        if a in scored["subset"].values and b in scored["subset"].values:
            ra = scored.loc[scored["subset"] == a].iloc[0]
            rb = scored.loc[scored["subset"] == b].iloc[0]
            if ra["auc"] < base["auc"] and rb["auc"] < base["auc"]:
                print(
                    f"\nBOTH halves of the {axis} split score below pooled "
                    f"({ra['auc']:.4f} and {rb['auc']:.4f} vs {base['auc']:.4f}). The pooled "
                    f"number is therefore partly the ease of separating these groups from each "
                    f"other, not skill at ranking within either. Treat the pooled AUC as a "
                    f"mixture, and quote within-cohort figures to anyone acting inside a cohort."
                )

    t.to_csv(DATASETS / "subset_discrimination.csv", index=False)
    print("\nsaved datasets/subset_discrimination.csv")

    if ledger:
        d = scored.set_index("subset")
        metrics = {"pooled_auc": round(float(base["auc"]), 4)}
        for s in ("activated", "not activated", "pro", "sender", "viewer", "icp office", "non-icp office"):
            if s in d.index:
                metrics[f"auc_{s.replace(' ', '_').replace('-', '_')}"] = round(float(d.loc[s, "auc"]), 4)
                metrics[f"n_{s.replace(' ', '_').replace('-', '_')}"] = int(d.loc[s, "n"])
        harness.append_ledger(
            "subset discrimination reads (activated / pro / ICP)",
            "oof_cv5",
            "hgb",
            metrics,
            notes="DATA-2241 item 2. AUC is not comparable across subsets (moves with base rate "
            "and n), so every figure carries a bootstrap CI and its base rate. Differences "
            "are only claimed where a subset CI excludes the pooled AUC.",
            area="area4",
            ver=WS_VERSION,
            model_id=CANON_MODEL_ID,
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", action="store_true")
    main(ap.parse_args().ledger)
