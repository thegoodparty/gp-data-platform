"""DATA-2238 Area 1, item 5: SPIKE. Can in-cycle rows enter training instead of being excluded?

ANSWER: NO. The filter stays. Read the rest of this docstring for why the obvious shortcut fails.

The builder drops every row whose election has not happened (`general_election_date <
current_date()`), which costs roughly 10.5k rows, all of them drawn from exactly the population the
model is meant to score. The original framing was survival analysis: treat those rows as censored
and model the hazard of corroboration. DATA-2312 appeared to make that unnecessary by establishing
that corroboration is a ROSTER process, not a results process, completing a median of 63 days
BEFORE election day. Read quickly, that says the excluded rows already have final labels and the
filter is merely too conservative.

That reading is wrong, and this spike is the correction. "Median 63 days before election day"
describes WHEN arrivals happen among rows that eventually corroborate. It says nothing about how
much of a given row's label has landed at a given moment, which is the actual question. Turned into
a maturity curve, the same arrivals are brutally late: only about 8.5% of eventual corroborations
are present 90 days before election day, 37% at 60 days, and 93% at 30. Meanwhile 88% of the
excluded rows sit 61-90 days out, so admitting them on observed labels would inject roughly 9,000
rows marked 0 that are mostly just not-yet-corroborated. A maturity-gated rule that waits for 30-day
maturity admits 24 rows, so there is nothing cheap here either.

What the curve IS good for is in-cycle base-rate correction: an observed 3.0% corroboration rate at
this distance implies a final rate near 27-35%. That is the machinery the in-cycle calibration
problem needs. Censoring models remain the only route to using these rows for TRAINING, and at 8.5%
maturity a censored row has barely entered its event window, so the information content is low.

Clock and its limits. The only channel with a real arrival timestamp is BallotReady
(`candidacy_created_at` on the S3 candidacies feed). TechSpeed has none, and DDHQ is not a parallel
live channel at all: per the `candidacy_stage` column comments, `source_candidate_id` carries the
DDHQ id for 2025-archive rows and the BallotReady id for 2026+, so DDHQ is an ERA, not an
alternative source of in-cycle corroboration. Since the scoring population is 2026+, BR is the
right clock; the script reports channel mix so that assumption stays visible rather than assumed.
`candidacy_created_at` is BR's own record date, so it measures when a fact became knowable, not
when it reached our warehouse; ingestion lag sits on top and is not measured here.

Run (from analytics/):
  uv run --with pyarrow python projects/legit_candidate_predictor/spike_label_maturity.py
"""

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from databricks_conn import run_query  # noqa: E402

DATASETS = HERE / "datasets"
CACHE = DATASETS / "label_maturity_probe.parquet"

# Mirrors build_working_set.py's population and race-deadline derivation, with the
# `general_election_date < current_date()` filter REMOVED so the excluded in-cycle rows
# are visible. Nothing here is feature-eligible; this is a diagnostic population.
SQL = """
with corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null then 1 else 0 end)        as has_br,
         max(case when ts_source_candidate_id is not null then 1 else 0 end) as has_ts,
         max(case when ddhq_candidate_id is not null then 1 else 0 end)      as has_ddhq
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
br_arrival as (
  select cs.gp_candidacy_id,
         min(cast(cv.candidacy_created_at as date)) as br_arrival_on
  from goodparty_data_catalog.mart_civics.candidacy_stage cs
  join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_candidacies_v3 cv
    on cast(cv.br_candidacy_id as string) = cast(cs.br_candidacy_id as string)
  where cs.br_candidacy_id is not null
  group by cs.gp_candidacy_id
),
fperiod as (
  select database_id, start_on, end_on
  from goodparty_data_catalog.dbt.int__ballotready_filing_period
),
fp_ids as (
  select r.database_id as race_database_id,
         max(fp.databaseid) as filing_period_database_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  lateral view explode(r.filing_periods) as fp
  group by r.database_id
),
race_general as (
  select r.database_id as br_race_id,
         r.position.databaseid as br_position_id,
         e.election_day
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_race r
  join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_election e
    on r.election.databaseid = e.database_id
  where not coalesce(r.is_primary, false)
    and not coalesce(r.is_runoff, false)
    and not coalesce(r.is_recall, false)
),
race_deadline as (
  select rg.br_position_id, rg.election_day,
         max(p.end_on) as filing_deadline
  from race_general rg
  left join fp_ids fi on fi.race_database_id = rg.br_race_id
  left join fperiod p on p.database_id = fi.filing_period_database_id
  group by 1, 2
)
select
  u.campaign_id,
  c.gp_candidacy_id,
  c.general_election_date,
  rd.filing_deadline,
  cast(u.user_created_at as date) as signup_date,
  coalesce(k.has_br, 0)   as has_br,
  coalesce(k.has_ts, 0)   as has_ts,
  coalesce(k.has_ddhq, 0) as has_ddhq,
  b.br_arrival_on
from goodparty_data_catalog.mart_analytics.users_win_candidacy u
join goodparty_data_catalog.mart_civics.candidacy c
  on u.campaign_id = c.product_campaign_id
 and u.election_date = c.general_election_date
left join corrob k on c.gp_candidacy_id = k.gp_candidacy_id
left join br_arrival b on b.gp_candidacy_id = c.gp_candidacy_id
left join race_deadline rd
  on rd.br_position_id = cast(u.ballotready_position_id as bigint)
 and rd.election_day = c.general_election_date
where u.is_latest_version
  and not u.is_demo
  and c.general_election_date >= date'2025-01-01'
"""


def load(refresh: bool) -> pd.DataFrame:
    if CACHE.exists() and not refresh:
        print(f"using cache {CACHE.name} (pass --refresh to re-query)")
        return pd.read_parquet(CACHE)
    df = run_query(SQL)
    DATASETS.mkdir(exist_ok=True)
    df.to_parquet(CACHE, index=False)
    print(f"queried and cached -> {CACHE.name}")
    return df


def main(refresh: bool, ledger: bool) -> None:
    df = load(refresh)
    for c in ("general_election_date", "filing_deadline", "signup_date", "br_arrival_on"):
        df[c] = pd.to_datetime(df[c])
    today = pd.Timestamp.today().normalize()

    df["corroborated"] = (df[["has_br", "has_ts", "has_ddhq"]].sum(axis=1) > 0).astype(int)
    df["is_mature"] = df["general_election_date"] < today
    df["cycle"] = np.where(df["general_election_date"].dt.year <= 2025, "2025 and earlier", "2026+")

    print(
        f"\npopulation: {len(df):,} rows  "
        f"({int(df.is_mature.sum()):,} mature, {int((~df.is_mature).sum()):,} in-cycle)"
    )
    print("\n=== corroboration channel mix (corroborated rows only) ===")
    corr = df[df["corroborated"] == 1]
    print(
        pd.crosstab(
            corr["cycle"],
            [corr["has_br"], corr["has_ts"], corr["has_ddhq"]],
            rownames=["cycle"],
            colnames=["br", "ts", "ddhq"],
        ).to_string()
    )
    print(
        "\nDDHQ is a 2025-archive era, not a live in-cycle channel. If the 2026+ row shows "
        "DDHQ-only corroboration in any volume, this spike's BR-clock assumption is wrong."
    )

    # --- maturity curve, measured where the label is known and the clock exists ---
    m = df[df["is_mature"] & (df["has_br"] == 1) & df["br_arrival_on"].notna()].copy()
    m["lead_days_election"] = (m["general_election_date"] - m["br_arrival_on"]).dt.days
    m["lag_days_deadline"] = (m["br_arrival_on"] - m["filing_deadline"]).dt.days

    print(f"\n=== BR arrival clock on mature elections (n={len(m):,}) ===")
    q = m["lead_days_election"].quantile([0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.95]).round(0)
    print("days BEFORE election day that the BR record appeared:")
    print(q.to_string())
    print(f"share arriving before election day: {(m['lead_days_election'] > 0).mean():.3%}")

    dl = m["lag_days_deadline"].dropna()
    print(f"\ndays AFTER the filing deadline (n={len(dl):,}):")
    print(dl.quantile([0.1, 0.25, 0.5, 0.75, 0.9, 0.95]).round(0).to_string())
    print(f"share arriving after the deadline: {(dl > 0).mean():.1%}")

    # Maturity curve: of all eventual BR corroborations, what share has landed by D days out?
    print("\n=== maturity curve: share of eventual BR corroborations already present ===")
    rows = []
    for d in (180, 150, 120, 90, 60, 45, 30, 14, 0):
        rows.append(
            {"days_before_election": d, "share_arrived": float((m["lead_days_election"] >= d).mean())}
        )
    curve = pd.DataFrame(rows)
    print(curve.round(3).to_string(index=False))

    # Robustness: the pooled curve mixes the DDHQ-heavy 2025 archive era with 2026+. The
    # conclusion only stands if BR's arrival behaviour is comparable across them.
    m["cycle_grp"] = np.where(m["general_election_date"].dt.year <= 2025, "2025", "2026+")
    split = pd.DataFrame(
        [
            {
                "days_before": d,
                **{
                    cy: round(float((g["lead_days_election"] >= d).mean()), 3)
                    for cy, g in m.groupby("cycle_grp")
                },
            }
            for d in (120, 90, 60, 45, 30, 14)
        ]
    )
    print("\n--- same curve split by cycle (robustness) ---")
    print(split.to_string(index=False))
    print(
        f"n by cycle: {m.groupby('cycle_grp').size().to_dict()}, "
        f"median lead: {m.groupby('cycle_grp')['lead_days_election'].median().to_dict()}"
    )

    # --- what admitting them would buy ---
    inc = df[~df["is_mature"]].copy()
    inc["days_to_election"] = (inc["general_election_date"] - today).dt.days

    def maturity_at(days: float) -> float:
        """Share of eventual BR corroborations present `days` before election day."""
        return float((m["lead_days_election"] >= days).mean())

    edges = [0, 14, 30, 60, 90, 120, 180, 10**6]
    labels = ["<=14d", "15-30d", "31-60d", "61-90d", "91-120d", "121-180d", ">180d"]
    print(f"\n=== currently-excluded in-cycle rows ({len(inc):,}) by distance to election ===")
    rows = []
    for lo, hi, lab in zip(edges[:-1], edges[1:], labels, strict=False):
        g = inc[(inc["days_to_election"] > lo - 1) & (inc["days_to_election"] <= hi)]
        if g.empty:
            continue
        obs = float(g["corroborated"].mean())
        # Maturity is looked up per band edge, never by position: an empty band used to shift
        # the whole column by one and silently mislabel every row.
        near, far = maturity_at(max(lo, g["days_to_election"].min())), maturity_at(hi)
        rows.append(
            {
                "band": lab,
                "n": len(g),
                "observed_rate": obs,
                "maturity_near": near,
                "maturity_far": far,
                "implied_final_lo": min(1.0, obs / near) if near > 0 else np.nan,
                "implied_final_hi": min(1.0, obs / far) if far > 0 else np.nan,
            }
        )
    print(pd.DataFrame(rows).round(3).to_string(index=False))
    print(
        "\nobserved_rate on an immature row is a FLOOR: records still to arrive can only push it "
        "up. implied_final divides it by the maturity at each band edge, so it is a RANGE and it "
        "is unstable wherever maturity is small; read it as an order of magnitude, not a number."
    )

    for cut, name in ((14, "14-day"), (30, "30-day"), (90, "90-day")):
        n = int((inc["days_to_election"] <= cut).sum())
        print(
            f"admissible under a {name} rule (maturity {maturity_at(cut):.1%}): "
            f"{n:,} of {len(inc):,} ({n / len(inc):.1%})"
        )

    top_date = inc["general_election_date"].value_counts()
    print(
        f"\nin-cycle rows are effectively ONE election: {top_date.index[0].date()} carries "
        f"{top_date.iloc[0]:,} of {len(inc):,} ({top_date.iloc[0] / len(inc):.1%}). They all "
        f"mature together, so there is no staggered admission to exploit."
    )

    if ledger:
        sys.path.insert(0, str(HERE))
        import harness

        harness.append_ledger(
            "SPIKE: label maturity / in-cycle admission",
            "other",
            "none",
            {
                "n_in_cycle_excluded": int(len(inc)),
                "maturity_at_90d": round(maturity_at(90), 4),
                "maturity_at_30d": round(maturity_at(30), 4),
                "median_lead_days": float(m["lead_days_election"].median()),
                "share_arriving_pre_election": round(float((m["lead_days_election"] > 0).mean()), 5),
                "admissible_30d_rule": int((inc["days_to_election"] <= 30).sum()),
                "largest_election_share": round(float(top_date.iloc[0] / len(inc)), 4),
            },
            notes="DATA-2238 item 5. ANSWER: NO, in-cycle rows cannot enter training on observed "
            "labels. 88% sit 61-90d from their election where only 8.5-11% of eventual BR "
            "corroborations have arrived; admitting them would inject ~9k mostly-mislabeled "
            "zeros. A maturity-gated rule admits 24 rows, so there is nothing cheap here. "
            "The curve's real use is in-cycle base-rate correction, not training augmentation.",
            area="area1",
            ver="b04cdd5ac4b1",
        )
        print("ledger row appended")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--ledger", action="store_true")
    a = ap.parse_args()
    main(a.refresh, a.ledger)
