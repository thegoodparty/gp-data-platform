"""Build the working set for the Win engagement report, 2026 cycle.

Spec: win_engagement_2026_brief.yaml in this folder. Writes parquet files to datasets/; analyze.py
slices them in pandas and runs no queries.

Run from analytics/:
    uv run --with pyarrow python -u projects/win_engagement_2026/build_working_set.py [name ...]
"""

from __future__ import annotations

import sys
import time
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))

import databricks_conn as dbc  # noqa: E402

OUT = HERE / "datasets"
CAT = "goodparty_data_catalog"

# Cycles are set by election date. 2025 starts in September so the cycle holds the fall races only.
CYCLE_2025 = ("2025-09-01", "2025-12-31")
CYCLE_2026 = ("2026-01-01", "2026-11-03")
SIGNUP_START = "2025-09-01"
PU_SCHEMA = "dbt"


def population_cte() -> str:
    # Name, email and phone stay in the warehouse; email is read only to drop staff, which
    # is_internal does not fully cover.
    return f"""
corroborated as (
  select g.campaign_id, max(v.has_vendor) as is_corroborated
  from (
    -- Stage rows carry the product campaign id only from 2026; the candidacy mart covers 2025.
    select cast(gp_api_campaign_id as string) as campaign_id, gp_candidacy_id
    from {CAT}.mart_civics.candidacy_stage where gp_api_campaign_id is not null
    union
    select cast(product_campaign_id as string), gp_candidacy_id
    from {CAT}.mart_civics.candidacy where product_campaign_id is not null
  ) g
  join (
    select gp_candidacy_id,
      max(br_candidacy_id is not null or ts_source_candidate_id is not null
          or ddhq_candidate_id is not null) as has_vendor
    from {CAT}.mart_civics.candidacy_stage group by 1
  ) v using (gp_candidacy_id)
  group by 1
),
pop as (
  select
    j.user_id, j.registered_at, j.product,
    j.candidacy_election_date as election_date,
    case
      when j.candidacy_election_date between '{CYCLE_2025[0]}' and '{CYCLE_2025[1]}' then 2025
      when j.candidacy_election_date between '{CYCLE_2026[0]}' and '{CYCLE_2026[1]}' then 2026
    end as cycle,
    j.candidacy_state as state, j.candidacy_office_level_normalized as office_level,
    j.candidacy_voter_count_bucket as voter_count_bucket,
    coalesce(j.candidacy_is_win_icp, false) as is_win_icp,
    j.candidacy_is_incumbent as is_incumbent, j.candidacy_is_uncontested as is_uncontested,
    j.candidacy_result as result,
    j.acquisition_motion, j.hs_analytics_source, j.first_touch_at, j.first_touch_is_signup,
    coalesce(j.is_activated, false) as is_activated, j.activated_at, j.first_activation_channel,
    j.onboarding_started_at, j.onboarding_completed_at, coalesce(j.is_onboarded, false) as is_onboarded,
    coalesce(j.is_pro, false) as is_pro, j.pro_since, j.pro_since_source, j.first_subscription_payment_at,
    j.has_amplitude_data, j.first_activity_at, j.last_activity_at, j.active_days_total,
    coalesce(j.is_power_user, false) as is_power_user_v1, j.usage_group,
    j.outreach_sent_count, j.outreach_first_sent_at, j.outreach_last_sent_at,
    -- Power user v2 is read upstream because no mart carries it yet. Its cycle is the election's
    -- calendar year, so it is kept only where that matches this user's election.
    coalesce(pu.is_power_user and pu.power_user_cycle = year(j.candidacy_election_date), false)
      as is_power_user,
    pu.power_user_action_days, pu.power_user_action_areas,
    -- Viability updates on contest facts only, so reading it after the race does not leak outcome.
    cp.candidacy_viability_score as viability_score,
    -- DATA-2202 corroborated: a BallotReady, TechSpeed or DDHQ record for this candidacy. Every
    -- race in both cycles is past its election date by AS_OF, so the timing half of that rule holds.
    coalesce(cs.is_corroborated, false) as is_corroborated
  from {CAT}.mart_analytics.user_journey j
  left join {CAT}.{PU_SCHEMA}.int__user_power_user_v2 pu on pu.user_id = j.user_id
  left join {CAT}.dbt.int__user_candidacy_profile cp on cp.user_id = j.user_id
  left join corroborated cs on cs.campaign_id = cast(cp.candidacy_campaign_id as string)
  where j.product in ('win', 'both')
    and not coalesce(j.is_internal, false)
    and not coalesce(j.is_demo_only, false)
    and not coalesce(lower(j.email) like '%@goodparty.org', false)
    and (j.registered_at >= '{SIGNUP_START}'
         or j.candidacy_election_date between '{CYCLE_2025[0]}' and '{CYCLE_2026[1]}')
)"""


def users_sql() -> str:
    return f"with {population_cte()} select * from pop"


def weekly_sql() -> str:
    return f"""
with {population_cte()}
select w.user_id, w.week_start_date, w.win_activity_days
from {CAT}.mart_analytics.user_win_activity_weekly w
join pop using (user_id)"""


def area_monthly_sql() -> str:
    return f"""
with {population_cte()}
select a.user_id, a.product_area, a.activity_month, a.area_active_days
from {CAT}.mart_analytics.user_product_area_activity_monthly a
join pop using (user_id)
where a.product = 'win'"""


def hourly_sql() -> str:
    # Upstream of the marts on purpose: no mart keeps the hour of an event, and local hour needs it.
    # Same rule as user_win_activity_weekly (a non-machine Win event). Activated users only: the
    # day-of-week and hour views are read for the three cuts, all of them activated.
    return f"""
with {population_cte()}
select try_cast(e.user_id as bigint) as user_id,
  date_trunc('hour', e.event_time) as hour_utc, count(*) as events
from {CAT}.dbt.int__amplitude_events_classified e
join pop p on p.user_id = try_cast(e.user_id as bigint)
where p.is_activated and p.cycle is not null
  and e.event_product = 'win' and not e.is_machine_emitted
  and e.event_time >= '2025-01-01'
group by 1, 2"""


def first_events_sql() -> str:
    # The first event of each active hour outside the dashboard and Pro banners, to see where people
    # go after a quiet spell: the dashboard is where Win opens, so landing there says nothing. Same
    # event rule and population as hourly_sql. Win events carry no referrer or UTM, so whether a
    # visit came from a link can't be seen here.
    return f"""
with {population_cte()}
select try_cast(e.user_id as bigint) as user_id, date_trunc('hour', e.event_time) as hour_utc,
  min_by(e.event_type, e.event_time) as event_type,
  min_by(e.product_area, e.event_time) as product_area
from {CAT}.dbt.int__amplitude_events_classified e
join pop p on p.user_id = try_cast(e.user_id as bigint)
where p.is_activated and p.cycle is not null
  and e.event_product = 'win' and not e.is_machine_emitted
  and e.event_time >= '2025-01-01'
  and coalesce(e.product_area, '') not in ('win_dashboard', 'win_pro_upgrade')
group by 1, 2"""


def touches_sql() -> str:
    # Meeting outcome is not modeled yet, so it is read from the raw engagements stream
    # (follow-up ticket: model HubSpot meeting and call outcome on user touches).
    return f"""
with {population_cte()}
select t.user_id, t.touch_id, t.channel, t.is_outbound_touch, t.is_connected_call, t.touch_at,
  t.call_outcome_family, coalesce(o.is_campaign_success, false) as is_cas,
  o.primary_team_name, e.`metadata_meetingOutcome` as meeting_outcome
from {CAT}.mart_analytics.user_hubspot_touches t
join pop using (user_id)
left join {CAT}.mart_analytics.hubspot_owner_teams o on o.hs_owner_id = cast(t.hubspot_owner_id as string)
left join {CAT}.airbyte_source.hubspot_api_engagements e on cast(e.id as string) = cast(t.touch_id as string)
where t.touch_at >= '2025-01-01'"""


def outreach_sql() -> str:
    # Replies and opt-outs come from the staged gp-api text interaction table, one row per
    # recipient per send, written from 2026-07-22.
    return f"""
with {population_cte()},
replies as (
  -- gp-api stamps responded_at on a STOP too, so a real reply is one without an opt-out.
  select outreach_id, count(*) as recipients,
    count(responded_at) as replied, count(opted_out_at) as opted_out,
    count(case when responded_at is not null and opted_out_at is null then 1 end) as replied_real
  from {CAT}.dbt.stg_airbyte_source__gp_api_db_contact_interaction_text
  group by 1
),
sends as (
  select distinct send_key, basis from {CAT}.dbt.int__user_outreach_sends
)
select a.outreach_id, a.user_id, a.outreach_type, a.status, a.send_at,
  a.voter_file_filter_id is not null as has_audience, a.demographic_dimension_count,
  a.is_own_audience, a.message_text is not null as has_text,
  p.purpose, p.purpose_source, coalesce(p.is_test, false) as is_test,
  r.recipients, r.replied, r.opted_out, r.replied_real,
  sd.basis, o.createdat as created_at, o.date as scheduled_date
from {CAT}.dbt.int__user_outreach_audiences a
join pop using (user_id)
left join {CAT}.dbt.int__outreach_purpose p on p.outreach_id = a.outreach_id
left join replies r on cast(r.outreach_id as string) = cast(a.outreach_id as string)
left join sends sd on sd.send_key = 'gp_api_outreach|' || cast(a.outreach_id as string)
left join {CAT}.dbt.stg_airbyte_source__gp_api_db_outreach o
  on cast(o.id as string) = cast(a.outreach_id as string)"""


def sends_sql() -> str:
    # The send model: every send from gp-api, Amplitude and polls, deduplicated, with its basis.
    # 2025 sends exist here mostly as Amplitude events, so send counts come from this, not outreach.
    return f"""
with {population_cte()}
select s.user_id, s.channel, s.send_at, s.source, s.basis, s.recipient_count, s.send_key
from {CAT}.dbt.int__user_outreach_sends s
join pop using (user_id)
where s.product = 'win'"""


def feature_days_sql() -> str:
    # Upstream on purpose: the day grain behind user_feature_use, needed for repeat use and return.
    return f"""
with {population_cte()}
select f.user_id, f.feature, f.activity_date
from {CAT}.dbt.int__user_feature_days f
join pop p using (user_id)
-- Onboarded non-senders are kept for the "furthest step toward a send" view.
where (p.is_activated or p.is_onboarded) and p.cycle is not null
  and f.is_user_action and not f.is_onboarding"""


def action_days_sql() -> str:
    # The day grain power user v2 counts. Pulled for every sender: the report ranks senders by
    # action days in the election year (its own top-15% power-user cut).
    return f"""
with {population_cte()}
select a.user_id, a.activity_date, a.has_send
from {CAT}.dbt.int__user_action_days a
join pop p using (user_id)
where p.is_activated and p.cycle is not null"""


def power_bar_sql() -> str:
    return f"select cycle, days_out, action_day_bar from {CAT}.{PU_SCHEMA}.int__power_user_bar_v2"


QUERIES = {
    "users": users_sql,
    "weekly": weekly_sql,
    "area_monthly": area_monthly_sql,
    "hourly": hourly_sql,
    "touches": touches_sql,
    "first_events": first_events_sql,
    "outreach": outreach_sql,
    "sends": sends_sql,
    "feature_days": feature_days_sql,
    "action_days": action_days_sql,
    "power_bar": power_bar_sql,
}


def main(names: list[str]) -> None:
    OUT.mkdir(exist_ok=True)
    run_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    print(f"run at {run_ts}")
    for name in names or list(QUERIES):
        t0 = time.time()
        df = dbc.run_query(QUERIES[name]())
        df.to_parquet(OUT / f"{name}.parquet", index=False)
        print(f"{name}: {len(df):,} rows in {time.time() - t0:.0f}s")
    if not names:  # a partial refresh keeps the full pull's timestamp
        (OUT / "run_ts.txt").write_text(run_ts)


if __name__ == "__main__":
    main(sys.argv[1:])
