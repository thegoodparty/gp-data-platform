"""Segment columns for the Win Pro pricing-segments sub-analysis.

Spec: win_pro_pricing_segments_brief.yaml. Rebuilds the votes-needed working set in the same run
(so both share one timestamp), then adds one row per user of purchase-time dimensions from
user_journey. Writes datasets/segments.parquet and datasets/cas_dates.parquet
(`--cas-dates-only` refreshes the latter pinned to the existing run).

Run from analytics/:
    uv run --with pyarrow python projects/win_pro_votes_needed/build_segments.py
"""

from __future__ import annotations

import sys

import pandas as pd
from build_working_set import CAT, OUT, dbc, population_sql
from build_working_set import main as build_working_set


def segments_sql() -> str:
    return f"""
with {population_sql()},
team as (
  select try_cast(user_id as bigint) as user_id, count(*) as team_invites
  from {CAT}.dbt.stg_airbyte_source__amplitude_api_events
  where event_type = 'Team - Member Invited'
  group by 1
),
owners as (
  select cast(id as string) as owner_id, teams from {CAT}.dbt.stg_airbyte_source__hubspot_api_owners
  union all
  select cast(id as string), teams from {CAT}.dbt.stg_airbyte_source__hubspot_api_owners_archived
),
-- CaS ("Campaign Success" in HubSpot). Every upgrade is offered a CaS call, so contact, not ownership,
-- is what separates customers. Counted up to the end of election day.
cas as (
  select p.user_id,
    count_if(t.channel = 'call') as cas_calls,
    count_if(t.channel in ('email', 'sms')) as cas_msgs,
    count_if(t.channel = 'meeting') as cas_meetings,
    min(t.touch_at) as first_cas_at
  from pop p
  join {CAT}.dbt.int__user_hubspot_touches t
    on t.user_id = p.user_id and t.is_outbound_touch and t.channel in ('call', 'email', 'sms', 'meeting')
   and t.touch_at < p.we
  where t.hubspot_owner_id in (
    select owner_id from owners where exists(teams, x -> x.name = 'Campaign Success'))
  group by 1
),
first_send as (
  select p.user_id, min(s.send_at) as first_send_at
  from pop p join {CAT}.dbt.int__user_outreach_sends s
    on s.user_id = p.user_id and s.product = 'win' and s.basis = 'committed'
   and s.send_at >= p.ws and s.send_at < p.we
  group by 1
)
select p.user_id,
  j.candidacy_office_level_normalized as office_level_norm,
  j.candidacy_office_category as office_category,
  j.candidacy_state as state,
  j.candidacy_is_incumbent as is_incumbent,
  j.election_year_count,
  j.candidacy_partisan_type as partisan_type,
  j.candidacy_is_uncontested as is_uncontested,
  j.candidacy_is_uncontested_is_estimated as uncontested_is_estimated,
  j.candidacy_number_of_opponents as number_of_opponents,
  j.candidacy_number_of_opponents_is_estimated as opponents_is_estimated,
  j.candidacy_seats_available as seats_available,
  -- pro_since is null for every user who canceled after their election; the first subscription
  -- payment is the same date where both exist and is set for the whole population.
  j.first_subscription_payment_at,
  j.onboarding_completed_at,
  j.candidacy_is_win_icp as is_win_icp,
  j.acquisition_motion,
  j.hs_analytics_source,
  coalesce(t.team_invites, 0) as team_invites,
  coalesce(c.cas_calls, 0) as cas_calls, coalesce(c.cas_msgs, 0) as cas_msgs,
  coalesce(c.cas_meetings, 0) as cas_meetings, c.first_cas_at, fs.first_send_at
from pop p
join {CAT}.mart_analytics.user_journey j on j.user_id = p.user_id
left join team t on t.user_id = p.user_id
left join cas c on c.user_id = p.user_id
left join first_send fs on fs.user_id = p.user_id
"""


def cas_dates_sql(ws: pd.DataFrame) -> str:
    """When CaS contact first met the rule (the first call or meeting, or the second email or text), and
    the first text. Keyed on the working set's own users and windows and capped at its run, so it can be
    refreshed later without moving anything else (the live population drifts)."""
    run_ts = str(ws["run_ts"].iloc[0])
    rows = ",\n  ".join(
        f"({int(r.user_id)}, timestamp'{r.ws}', timestamp'{r.we}')"
        for r in ws[["user_id", "ws", "we"]].itertuples()
    )
    return f"""
with pop as (
  select * from values
  {rows}
  as v(user_id, ws, we)
),
owners as (
  select cast(id as string) as owner_id, teams from {CAT}.dbt.stg_airbyte_source__hubspot_api_owners
  union all
  select cast(id as string), teams from {CAT}.dbt.stg_airbyte_source__hubspot_api_owners_archived
),
t as (
  select p.user_id, t.channel, t.touch_at,
    row_number() over (
      partition by p.user_id, t.channel in ('email', 'sms') order by t.touch_at) as nth
  from pop p
  join {CAT}.dbt.int__user_hubspot_touches t
    on t.user_id = p.user_id and t.is_outbound_touch and t.channel in ('call', 'email', 'sms', 'meeting')
   and t.touch_at < p.we and t.touch_at <= timestamp'{run_ts}'
  where t.hubspot_owner_id in (
    select owner_id from owners where exists(teams, x -> x.name = 'Campaign Success'))
),
cas as (
  select user_id,
    count_if(channel = 'call') as cas_calls_check,
    count_if(channel in ('email', 'sms')) as cas_msgs_check,
    least(
      min(case when channel in ('call', 'meeting') then touch_at end),
      min(case when channel in ('email', 'sms') and nth = 2 then touch_at end)) as cas_qualified_at
  from t group by 1
),
first_text as (
  select p.user_id, min(s.send_at) as first_text_at
  from pop p join {CAT}.dbt.int__user_outreach_sends s
    on s.user_id = p.user_id and s.product = 'win' and s.basis = 'committed' and s.channel = 'texting'
   and s.send_at >= p.ws and s.send_at < p.we and s.send_at <= timestamp'{run_ts}'
  group by 1
)
select p.user_id, c.cas_calls_check, c.cas_msgs_check, c.cas_qualified_at, f.first_text_at
from pop p left join cas c on c.user_id = p.user_id left join first_text f on f.user_id = p.user_id
"""


def main() -> None:
    if "--cas-dates-only" in sys.argv:
        ws = pd.read_parquet(OUT / "working_set.parquet")
        run_ts = str(ws["run_ts"].iloc[0])
        d = dbc.run_query(cas_dates_sql(ws))
        d["run_ts"] = run_ts
        d.to_parquet(OUT / "cas_dates.parquet", index=False)
        print(f"cas dates: {len(d):,} users, {d['cas_qualified_at'].notna().sum():,} met the contact rule")
        return
    build_working_set()
    run_ts = pd.read_parquet(OUT / "working_set.parquet")["run_ts"].iloc[0]
    seg = dbc.run_query(segments_sql())
    seg["run_ts"] = run_ts
    seg.to_parquet(OUT / "segments.parquet", index=False)
    print(f"segments: {len(seg):,} rows, {seg['user_id'].nunique():,} distinct users")
    d = dbc.run_query(cas_dates_sql(pd.read_parquet(OUT / "working_set.parquet")))
    d["run_ts"] = run_ts
    d.to_parquet(OUT / "cas_dates.parquet", index=False)


if __name__ == "__main__":
    main()
