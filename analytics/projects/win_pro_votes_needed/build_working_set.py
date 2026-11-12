"""Build the per-user working set for Win Pro usage by votes needed to win (DATA-2809).

Spec: win_pro_votes_needed_brief.yaml in this folder. Writes three parquet files to datasets/;
the notebook slices them in pandas and runs no further queries.

Run from analytics/:
    uv run --with pyarrow python projects/win_pro_votes_needed/build_working_set.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))

import databricks_conn as dbc  # noqa: E402

OUT = HERE / "datasets"
CAT = "goodparty_data_catalog"
ELECTION_YEAR = 2026
WINDOW_MONTHS = 6

# Same action events as DATA-2690 (win_usage_archetypes/build_working_set.py), so "acted in an
# area" means the same thing in both analyses.
ACTION_EVENTS = (
    "'Outreach - Campaign Created','Voter Outreach - Campaign Scheduled','Robocall - Scheduled',"
    "'Outreach - Door Knocking List Created','Outreach - Phone Banking Call List Created',"
    "'Voter Outreach - Phone Banking Call List Created','Outreach - Draft Saved',"
    "'Outreach - Door Knocking Session Started','Outreach - Phone Banking Call Logged','Team - Outreach Assigned',"
    "'Voter Data - List Created','Contacts - Segment Created','Contacts - Download','Voter Data - List Exported',"
    "'Voter Data - Contact Status Changed','Voter Data - Note Added',"
    "'AI Chat - Message Sent','AI Assistant: Ask a question','Content Builder: Click Generate',"
    "'Contacts - Assistant Message Sent','Chief Of Staff - Document Attached',"
    "'Campaign Story - Rewrite Requested','Profile - Policy Priorities: Submit Add',"
    "'Profile - Policy Priorities: Submit Edit',"
    "'Win - Opponent Research Started','Win - Opponents Manually Added','Win - Opponent Standout Action Clicked',"
    "'Win - Opponent Profile Viewed'"
)


def population_sql() -> str:
    # One row per user (user_journey is user grain, its candidacy is the latest one). Pro today, or a
    # subscription that ended on or after election day; never-paid (comped) Pro excluded.
    return f"""
cand as (
  select j.user_id, j.candidacy_campaign_id as campaign_id, j.registered_at as user_created_at,
    j.candidacy_election_level as election_level, j.candidacy_voter_count as voter_count,
    coalesce(j.candidacy_general_election_date, j.candidacy_election_date) as ged,
    j.candidacy_general_election_date is null as ged_from_campaign,
    coalesce(j.is_pro, false) as pro_now, coalesce(j.is_activated, false) as is_activated
  from {CAT}.mart_analytics.user_journey j
  where not coalesce(j.is_internal, false)
    and year(coalesce(j.candidacy_general_election_date, j.candidacy_election_date)) = {ELECTION_YEAR}
    and (coalesce(j.is_pro, false)
         or j.subscription_ended_at >= coalesce(j.candidacy_general_election_date, j.candidacy_election_date))
    and j.first_subscription_payment_at is not null
),
pop as (
  select c.*, cp.organization_slug, cp.ballotready_race_id,
    cast(add_months(c.ged, -{WINDOW_MONTHS}) as timestamp) as ws,
    -- Election day itself is in: get-out-the-vote texts go out that day.
    cast(date_add(c.ged, 1) as timestamp) as we
  from cand c
  join {CAT}.mart_civics.campaigns cp
    on cp.campaign_id = c.campaign_id and cp.is_latest_version and not coalesce(cp.is_demo, false)
)"""


def working_set_sql(run_ts: str) -> str:
    # Votes needed follows gp-api's live rule (CampaignsService.fetchLiveRaceTargetMetrics):
    # override district, then the picked race, then the organization's position. The election-api
    # mirrors (dbt.m_election_api__*) have no mart equivalent.
    return f"""
with {population_sql()},
org as (select organization_slug, position_id, override_district_id from {CAT}.mart_civics.organizations),
pt as (
  select district_id, election_year,
    coalesce(max(case when election_code ilike '%general%' then projected_turnout end),
             max(projected_turnout)) as turnout
  from {CAT}.dbt.m_election_api__projected_turnout group by 1, 2
),
wn as (
  select p.user_id,
    case when o.override_district_id is not null and pto.turnout > 0 then floor(pto.turnout / 2) + 1 end as wn_override,
    o.override_district_id is not null as has_override,
    case when r.projected_turnout > 0 then floor(r.projected_turnout / 2) + 1 end as wn_race,
    case when ptp.turnout > 0 then ceil(ptp.turnout * 0.5) + 1 end as wn_position
  from pop p
  left join org o on o.organization_slug = p.organization_slug
  left join pt pto on pto.district_id = o.override_district_id and pto.election_year = year(p.ged)
  left join {CAT}.dbt.m_election_api__race r on r.br_database_id = p.ballotready_race_id
  left join {CAT}.dbt.m_election_api__position pos on pos.id = o.position_id
  left join pt ptp on ptp.district_id = pos.district_id and ptp.election_year = year(p.ged)
),
sends as (
  -- user_journey_events carries one row per committed send (DATA-2477), text count in event_value.
  select p.user_id,
    count(*) as sends_total,
    count_if(e.event_label = 'texting') as sends_texting,
    count_if(e.event_label = 'robocall') as sends_robocall,
    count_if(e.event_label = 'social') as sends_social,
    count_if(e.event_label = 'door_knocking') as sends_door_knocking,
    count_if(e.event_label = 'phone_banking') as sends_phone_banking,
    count_if(e.event_label = 'unattributed') as sends_unattributed,
    coalesce(sum(case when e.event_label = 'texting' then e.event_value end), 0) as texts_sent
  from pop p
  join {CAT}.mart_analytics.user_journey_events e
    on e.user_id = p.user_id and e.event = 'Outreach send'
   and e.event_at >= p.ws and e.event_at < p.we and e.event_at <= timestamp'{run_ts}'
  group by 1
),
money as (
  -- The Revenue dashboard's rule (journey-dashboard sql/rev_kpis.sql): Payment and Refund rows,
  -- primary Stripe customer row only, amounts already signed.
  select p.user_id,
    sum(case when e.event_label = 'outreach_purchase' then e.event_amount_usd else 0 end) as outreach_usd,
    sum(case when e.event_label = 'outreach_purchase' and e.event = 'Payment' then 1 else 0 end) as outreach_payments
  from pop p
  join {CAT}.mart_analytics.user_journey j on j.user_id = p.user_id
  join {CAT}.mart_analytics.user_journey_events e on e.user_id = p.user_id
  where coalesce(j.is_primary_stripe_customer_row, true) and e.event in ('Payment', 'Refund')
    and e.event_at >= p.ws and e.event_at < p.we and e.event_at <= timestamp'{run_ts}'
  group by 1
),
attempts as (
  -- Raw on purpose: no mart keeps unsent text campaigns. Any status, to separate "never tried"
  -- from "tried, never sent".
  select p.user_id, count(*) as p2p_started,
    count_if(o.status = 'pending_payment') as p2p_pending_payment
  from pop p
  join {CAT}.airbyte_source.gp_api_db_outreach o
    on o.campaignId = p.campaign_id and o.outreach_type = 'p2p' and o.createdAt >= p.ws and o.createdAt < p.we
  group by 1
)
select p.user_id, p.campaign_id, p.user_created_at, p.ged, p.ged_from_campaign, p.ws, p.we,
  p.pro_now, p.election_level, p.voter_count, p.ballotready_race_id is not null as has_race_id,
  wn.has_override, wn.wn_override, wn.wn_race, wn.wn_position,
  coalesce(s.sends_total, 0) as sends_total, coalesce(s.sends_texting, 0) as sends_texting,
  coalesce(s.sends_robocall, 0) as sends_robocall, coalesce(s.sends_social, 0) as sends_social,
  coalesce(s.sends_door_knocking, 0) as sends_door_knocking,
  coalesce(s.sends_phone_banking, 0) as sends_phone_banking,
  coalesce(s.sends_unattributed, 0) as sends_unattributed, coalesce(s.texts_sent, 0) as texts_sent,
  coalesce(m.outreach_usd, 0) as outreach_usd, coalesce(m.outreach_payments, 0) as outreach_payments,
  p.is_activated,
  coalesce(att.p2p_started, 0) as p2p_started, coalesce(att.p2p_pending_payment, 0) as p2p_pending_payment
from pop p
left join wn on wn.user_id = p.user_id
left join sends s on s.user_id = p.user_id
left join money m on m.user_id = p.user_id
left join attempts att on att.user_id = p.user_id
"""


def area_days_sql(run_ts: str) -> str:
    # DATA-2690's area mapping and sources, over this brief's window instead of signup-anchored.
    return f"""
with {population_sql()},
ev as (
  select p.user_id, date(e.event_time) as d,
    case
      when e.event_type like 'Voter Outreach - 10DLC%' or e.event_type like 'Outreach - Gate%'
        or e.event_type = 'Win - Opponent Upgrade Viewed' then null
      when e.event_type like 'Outreach%' or e.event_type like 'Voter Outreach%'
        or e.event_type like 'Schedule Text Campaign%' or e.event_type like 'Robocall%'
        or e.event_type like 'Door Knocking%' or e.event_type like 'Team - Outreach%' then 'outreach'
      when e.event_type like 'Contacts%' or e.event_type like 'Voter Data%' then 'voter_data'
      when e.event_type like 'AI Chat%' or e.event_type like 'AI Assistant%' or e.event_type like 'Chief Of Staff%'
        or e.event_type like 'Campaign Manager%' or e.event_type like 'Content Builder%' then 'ai'
      when e.event_type like 'Campaign Story%' or e.event_type like 'Profile - Policy Priorities%' then 'story'
      when e.event_type like 'Win - Opponent%' then 'opponents'
    end as area,
    case when e.event_type in ({ACTION_EVENTS}) then 1 else 0 end as act
  from pop p
  join {CAT}.dbt.stg_airbyte_source__amplitude_api_events e
    on try_cast(e.user_id as bigint) = p.user_id and e.event_time >= p.ws and e.event_time < p.we
   and e.event_time <= timestamp'{run_ts}'
  left join {CAT}.dbt.int__amplitude_event_catalog cat on cat.event_type = e.event_type
  where not coalesce(cat.is_machine_emitted, false)
),
db as (
  select p.user_id, date(r.createdAt) as d, 'outreach' as area, 1 as act
  from pop p join {CAT}.airbyte_source.gp_api_db_outreach r
    on r.organization_slug = p.organization_slug and r.createdAt >= p.ws and r.createdAt < p.we
  union all
  select p.user_id, date(v.created_at), 'voter_data', 1
  from pop p join {CAT}.airbyte_source.gp_api_db_voter_file_filter v
    on v.organization_slug = p.organization_slug and v.created_at >= p.ws and v.created_at < p.we
  union all
  select p.user_id, date(m.created_at), 'ai', 1
  from pop p
  join {CAT}.airbyte_source.gp_api_db_chat_conversation cc
    on cc.owner_user_id = p.user_id and cc.scope = 'campaign_assistant'
  join {CAT}.airbyte_source.gp_api_db_chat_message m
    on m.conversation_id = cc.id and m.role = 'user' and m.created_at >= p.ws and m.created_at < p.we
),
allx as (select user_id, d, area, act from ev where area is not null union all select user_id, d, area, act from db)
select user_id, area, count(distinct d) as days, count(distinct case when act = 1 then d end) as action_days
from allx group by 1, 2
"""


def main() -> None:
    OUT.mkdir(exist_ok=True)
    run_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    print(f"run timestamp (UTC): {run_ts}")
    ws = dbc.run_query(working_set_sql(run_ts))
    ws["run_ts"] = run_ts
    ws.to_parquet(OUT / "working_set.parquet", index=False)
    print(f"working set: {len(ws):,} users, {ws['user_id'].nunique():,} distinct")
    ad = dbc.run_query(area_days_sql(run_ts))
    ad["run_ts"] = run_ts
    ad.to_parquet(OUT / "area_days.parquet", index=False)
    print(f"area days: {len(ad):,} user-area rows")


if __name__ == "__main__":
    main()
