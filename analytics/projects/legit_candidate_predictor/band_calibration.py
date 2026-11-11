"""What does each band actually mean? Calibrate the live bands against labelled rows.

The scored live table has no labels, so nothing in it can tell you where to cut. This script
answers the question on the labelled population instead: apply the SAME cohort and banding
rule used by score_population.py to the working set, then measure what fraction of each band
actually corroborated. That turns "Q5" from a rank into a number you can put on a slide.

Reported three ways, because the right one depends on the claim:
  1. Overall band -> corroboration rate. The headline.
  2. Band within lead-time bucket. The transferable version: the live population is dominated
     by long-lead rows, whose base rate is far below the pooled one, so a pooled rate would
     overstate every band for them.
  3. Cumulative cuts (top 20%, top 40%, ...) with precision and recall, which is the shape an
     outreach decision actually takes.

Run: cd analytics && uv run --with pyarrow --with scikit-learn python \
       projects/legit_candidate_predictor/band_calibration.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import harness  # noqa: E402
from score_population import cohort_bands  # noqa: E402

OOF = HERE / "datasets" / "oof_br_position_gp_user_details_incumbency.parquet"
LIVE = HERE / "datasets" / "scored_population.parquet"


def main() -> None:
    ws = pd.read_parquet(harness.WORKING_SET)
    oof = pd.read_parquet(OOF)
    df = ws.merge(
        oof[["campaign_id", "corroborated", "oof_hgb"]],
        on="campaign_id",
        how="inner",
        validate="one_to_one",
        suffixes=("_ws", ""),
    )
    df["lead_time_days"] = (
        pd.to_datetime(df["general_election_date"]) - pd.to_datetime(df["signup_date"])
    ).dt.days
    p = df["oof_hgb"].to_numpy()
    b = cohort_bands(df, p)
    df = pd.concat([df.reset_index(drop=True), b.reset_index(drop=True)], axis=1)
    df["lead_bucket"] = df["cohort_key"].str.split(" | ", regex=False).str[0]
    y = df["corroborated"].astype(int)

    print(f"labelled rows {len(df):,}   overall corroboration {y.mean():.1%}\n")

    print("=== 1. band -> actual corroboration (pooled) ===")
    t = (
        df.groupby("band")
        .apply(
            lambda g: pd.Series(
                {
                    "n": len(g),
                    "corroborated": g["corroborated"].mean(),
                    "lift_vs_base": g["corroborated"].mean() / y.mean(),
                }
            ),
            include_groups=False,
        )
        .round(3)
    )
    print(t.to_string())

    print("\n=== 2. band within lead-time bucket (the transferable read) ===")
    piv = df.pivot_table(index="lead_bucket", columns="band", values="corroborated", aggfunc="mean").round(3)
    cnt = df.pivot_table(index="lead_bucket", columns="band", values="corroborated", aggfunc="size")
    order = ["lead_0_90d", "lead_91_180d", "lead_181_365d", "lead_365d_plus"]
    piv = piv.reindex([o for o in order if o in piv.index])
    cnt = cnt.reindex(piv.index)
    print("corroboration rate:")
    print(piv.to_string())
    print("\nrow counts:")
    print(cnt.to_string())
    print("\nbase rate per bucket:")
    print(
        df.groupby("lead_bucket")["corroborated"]
        .agg(["size", "mean"])
        .round(3)
        .reindex(piv.index)
        .to_string()
    )

    print("\n=== 3. cumulative cuts within cohort (what an outreach cut looks like) ===")
    rows = []
    for cut in (10, 20, 30, 40, 50, 60, 80):
        sel = df["score_pct_within_cohort"] >= (100 - cut)
        tp = int((sel & y.eq(1)).sum())
        rows.append(
            {
                "take_top_pct": cut,
                "n_selected": int(sel.sum()),
                "precision": round(tp / max(sel.sum(), 1), 3),
                "recall": round(tp / int(y.sum()), 3),
                "missed_real": int(y.sum()) - tp,
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== 4. same cuts, long-lead rows only (closest to the live population) ===")
    ll = df[df["lead_bucket"].eq("lead_365d_plus")]
    yl = ll["corroborated"].astype(int)
    print(f"n={len(ll):,}  base rate {yl.mean():.1%}")
    rows = []
    for cut in (10, 20, 30, 40, 50, 60, 80):
        sel = ll["score_pct_within_cohort"] >= (100 - cut)
        tp = int((sel & yl.eq(1)).sum())
        rows.append(
            {
                "take_top_pct": cut,
                "n_selected": int(sel.sum()),
                "precision": round(tp / max(sel.sum(), 1), 3),
                "recall": round(tp / max(int(yl.sum()), 1), 3),
            }
        )
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n=== 5. how the live population distributes across the same cohorts ===")
    live = pd.read_parquet(LIVE)
    live["lead_bucket"] = live["cohort_key"].str.split(" | ", regex=False).str[0]
    print(live.groupby("lead_bucket").size().reindex(order).to_string())


if __name__ == "__main__":
    main()
