"""DATA-2239 pass-A track: Amplitude product-behavior scaffold at the FAMILY grain.

PASS A ONLY — post-signup behavior is multi-causal with the outcome and never enters the
analysis filter (pass B). This is the catalog-4.8 design: aggregate on
`int__amplitude_event_catalog.family` (~15 stable families) instead of 526 brittle event
types, so columns survive event renames and new event types join an existing family without a
schema change. Built now because forward coverage is where it's strongest (92.3% of the 2026
in-cycle set has browser presence vs 44% historical) and it is the natural home for the
archetype work (Area 1).

Window: first 30 days after signup, so the columns mean the same thing for every user
regardless of account age (a lifetime aggregate would confound tenure with engagement).

Emits per family: event count + active days; overall: total events, active days, days to
first event, families touched. Raw counts (HGB bins; log at consumption if LR ever wants it).

Run: uv run --with pyarrow python projects/legit_candidate_predictor/build_amplitude_scaffold.py
"""

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "lib"))
from profile import write_profile  # noqa: E402

from databricks_conn import run_query  # noqa: E402
from intake import write_intake  # noqa: E402

SQL = """
with pop as (
  select cast(u.user_id as string)      as user_id,
         cast(u.user_created_at as date) as signup_date
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  where u.is_latest_version and not u.is_demo
  qualify row_number() over (partition by u.user_id
                             order by u.election_date desc nulls last, u.campaign_id desc) = 1
),
ev as (
  select p.user_id,
         coalesce(c.family, 'uncatalogued') as family,
         cast(e.event_time as date)         as d,
         e.event_time,
         p.signup_date
  from goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
  join pop p on cast(e.user_id as string) = p.user_id
  left join goodparty_data_catalog.dbt.int__amplitude_event_catalog c
    on c.event_type = e.event_type
  where cast(e.event_time as date) >= p.signup_date
    and cast(e.event_time as date) <= date_add(p.signup_date, 30)
)
select user_id, family,
       count(*)                                   as n_events,
       count(distinct d)                          as n_active_days,
       min(datediff(cast(event_time as date), signup_date)) as first_event_lag_days
from ev
group by grouping sets ((user_id, family), (user_id))
"""


def build(raw: pd.DataFrame, all_users: pd.Series) -> pd.DataFrame:
    overall = raw[raw["family"].isna()].set_index("user_id")
    fam = raw[raw["family"].notna()]

    wide = fam.pivot_table(index="user_id", columns="family", values="n_events", aggfunc="sum")
    wide.columns = [f"amp_{c}_events_30d" for c in wide.columns]

    out = pd.DataFrame({"user_id": all_users})
    out = out.merge(wide, left_on="user_id", right_index=True, how="left")
    out = out.merge(
        overall[["n_events", "n_active_days", "first_event_lag_days"]].rename(
            columns={
                "n_events": "amp_total_events_30d",
                "n_active_days": "amp_active_days_30d",
                "first_event_lag_days": "amp_days_to_first_event",
            }
        ),
        left_on="user_id",
        right_index=True,
        how="left",
    )
    out["amp_n_families_30d"] = wide.notna().reindex(out["user_id"]).sum(axis=1).to_numpy()

    # No browser/instrumented presence in the window is a GENUINE zero for the count columns
    # (the join is exhaustive over the events table), but the CRM-provenance finding says the
    # no-presence population is provenance-loaded, so the presence indicator stays explicit.
    out["amp_any_events_30d"] = out["amp_total_events_30d"].notna().astype(int)
    count_cols = [c for c in out.columns if c.endswith("_30d") and c != "amp_any_events_30d"]
    out[count_cols] = out[count_cols].fillna(0)
    return out


if __name__ == "__main__":
    raw = run_query(SQL)
    users = raw["user_id"].dropna().unique()
    print(f"users with any event rows: {len(users):,}")

    pop = run_query("""
        select cast(u.user_id as string) as user_id
        from goodparty_data_catalog.mart_analytics.users_win_candidacy u
        where u.is_latest_version and not u.is_demo
        qualify row_number() over (partition by u.user_id order by u.election_date desc
                                   nulls last, u.campaign_id desc) = 1""")
    feats = build(raw, pop["user_id"])
    feats["user_id"] = pd.to_numeric(feats["user_id"], errors="coerce").astype("Int64")
    feats = feats.dropna(subset=["user_id"])
    feats["user_id"] = feats["user_id"].astype("int64")
    print(f"users: {len(feats):,}  any events in 30d: {feats.amp_any_events_30d.mean():.1%}")

    out = HERE / "datasets" / "amplitude_family_scaffold.parquet"
    feats.to_parquet(out, index=False)
    print(f"saved -> {out}  ({feats.shape[1] - 1} columns)")

    write_profile(
        feats,
        "amplitude_family_scaffold",
        note="PASS A ONLY. Family-grain post-signup behavior, 30-day window. "
        "Columns stay stable as event types accumulate (event-catalog family "
        "grain). No-presence rows are genuine zeros with an explicit indicator "
        "(the no-browser population is CRM-provenance-loaded).",
    )
    write_intake(
        feats,
        "amplitude_family_scaffold",
        key="user_id",
        source="dbt.stg_airbyte_source__amplitude_api_events x "
        "int__amplitude_event_catalog (family grain)",
        join="user_id, events windowed [signup, signup+30d]",
        pit_class="A by construction (post-signup behavior) — NEVER enters pass B / " "the analysis filter",
    )
