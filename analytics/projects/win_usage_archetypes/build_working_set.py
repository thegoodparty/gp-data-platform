"""Build the per-user working set for the Win usage archetypes analysis (DATA-2690).

One row per eligible user with usage inputs, tech-comfort signals and context.
Spec: usage_archetypes_brief.yaml in this folder. Writes datasets/working_set.parquet
and datasets/chat_topics.parquet; every later cut is a pandas slice of these.

Run from analytics/:
    uv run --with scikit-learn --with pyarrow python projects/win_usage_archetypes/build_working_set.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "lib"))

import databricks_conn as dbc  # noqa: E402

OUT = HERE / "datasets"
SIGNUP_FLOOR = "2026-07-01"
MIN_DAYS = 14

TOPICS = [
    "campaign strategy",
    "voter outreach and messaging",
    "voter data and targeting",
    "fundraising",
    "filing, compliance and legal",
    "story, issues and positions",
    "opponents",
    "events and community",
    "how to use the app",
    "complaint or frustration",
    "other",
]

DASHBOARD_EVENTS = (
    "'Dashboard - Candidate Dashboard Viewed','Dashboard - Campaign Plan Viewed',"
    "'Campaign Plan - Campaign Tracker Viewed'"
)
# A start-day with no matching completion that day counts as an abandoned flow.
OUTREACH_DONE = (
    "'Outreach - Campaign Created','Outreach - Door Knocking List Created',"
    "'Outreach - Phone Banking Call List Created','Voter Outreach - Phone Banking Call List Created',"
    "'Voter Outreach - Campaign Scheduled','Robocall - Scheduled','Outreach - Draft Saved'"
)
LIST_DONE = "'Voter Data - List Created','Contacts - Segment Created'"
# Doing something in an area, as opposed to landing on its page. Drives journey order.
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
VOTER_DATA_VIEWS = "'Contacts - Contacts Viewed','Contacts - Segment Viewed','Voter Data - Contact Viewed'"


def population_sql(run_ts: str) -> str:
    return f"""
cand as (
  select user_id, min(user_created_at) as su, max(election_date) as election_date
  from goodparty_data_catalog.mart_analytics.users_win_candidacy
  where is_latest_version and not is_demo
    and coalesce(lower(user_email), '') not like '%@goodparty.org'
  group by user_id
  having min(user_created_at) >= timestamp'{SIGNUP_FLOOR} 00:00:00'
),
pop as (
  select c.user_id, c.su, c.election_date,
    least(coalesce(cast(c.election_date as timestamp), timestamp'{run_ts}'), timestamp'{run_ts}') as we
  from cand c
  join goodparty_data_catalog.mart_analytics.user_journey j on j.user_id = c.user_id
  where not coalesce(j.is_internal, false)
    and datediff(date(least(coalesce(cast(c.election_date as timestamp), timestamp'{run_ts}'), timestamp'{run_ts}')),
                 date(c.su)) >= {MIN_DAYS}
),
orgs as (
  select distinct p.user_id, cp.organization_slug, cp.id as campaign_id
  from pop p join goodparty_data_catalog.airbyte_source.gp_api_db_campaign cp
    on cp.user_id = p.user_id and not coalesce(cp.is_demo, false)
)"""


def working_set_sql(run_ts: str) -> str:
    return f"""
with {population_sql(run_ts)},
ev as (
  select p.user_id, e.event_type, e.event_time, date(e.event_time) as d, e.device_type, e.os_name,
    e.event_properties
  from pop p
  join goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
    on try_cast(e.user_id as bigint) = p.user_id
   and e.event_time >= p.su and e.event_time < p.we
  where e.event_time >= timestamp'{SIGNUP_FLOOR} 00:00:00'
),
human as (
  select ev.* from ev
  left join goodparty_data_catalog.dbt.int__amplitude_event_catalog cat on cat.event_type = ev.event_type
  where not coalesce(cat.is_machine_emitted, false) and ev.event_type not like '[Experiment]%'
),
ev_feat as (
  select user_id,
    count(distinct d) as active_days,
    max(case when event_type in ({DASHBOARD_EVENTS}) then 1 else 0 end) as any_dashboard,
    count(distinct case when event_type in ({VOTER_DATA_VIEWS}) then d end) as voter_data_days,
    avg(case when event_type = 'Voter Data - List Created'
        then try_cast(get_json_object(event_properties, '$.variableCount') as double) end) as list_filters_avg,
    count(distinct case when event_type in ('Onboarding V2 - Why Are You Running Completed',
        'Onboarding V2 - What''s Your Background Completed',
        'Onboarding V2 - What Issues Do You Want To Solve Completed') then event_type end) as story_steps_done,
    sum(case when event_type = 'Campaign Story - Rewrite Requested' then 1 else 0 end) as story_rewrites,
    sum(case when event_type like 'Profile - Policy Priorities: Submit%' then 1 else 0 end) as issue_edits,
    count(distinct case when event_type = 'Win - Opponent Profile Viewed' then d end) as opp_view_days,
    max(case when event_type = 'Win - Opponent Research Started' then 1 else 0 end) as opp_research_started,
    max(case when event_type = 'Win - Opponents Manually Added' then 1 else 0 end) as opp_manual_add,
    max(case when event_type = 'Win - Opponent Standout Action Clicked' then 1 else 0 end) as opp_action_clicked,
    count(distinct case when event_type in ('Campaign Plan - Campaign Tracker Viewed',
        'Dashboard - Campaign Plan Viewed') then d end) as plan_view_days,
    sum(case when event_type in ('Dashboard - Campaign Task Completed',
        'Campaign Plan - Task Action Taken') then 1 else 0 end) as plan_task_actions,
    sum(case when event_type like 'Content Builder: Generation Completed' then 1 else 0 end) as content_builder_gens,
    sum(case when event_type = 'Dictation - Started' then 1 else 0 end) as dictation_starts,
    max(case when event_type = 'Onboarding V2 - Onboarding Skipped' then 1 else 0 end) as onboarding_skipped,
    max(case when event_type = 'Candidate Website - Edited' then 1 else 0 end) as website_edited
  from human group by user_id
),
onb as (
  select p.user_id,
    max(case when e.event_time < p.su + interval 14 days then 1 else 0 end) as onboarded
  from pop p join ev e on e.user_id = p.user_id and e.event_type in ({DASHBOARD_EVENTS})
  group by p.user_id
),
onb_speed as (
  select user_id,
    (unix_timestamp(min(case when event_type = 'Onboarding V2 - Pledge Completed' then event_time end))
     - unix_timestamp(min(case when event_type = 'Onboarding V2 - Welcome Viewed' then event_time end))) / 60.0
      as onboarding_minutes
  from ev group by user_id
),
flow_days as (
  select user_id, d,
    max(case when event_type = 'Outreach - Click Create' then 1 else 0 end) as o_start,
    max(case when event_type in ({OUTREACH_DONE}) then 1 else 0 end) as o_done,
    max(case when event_type = 'Contacts - List Wizard Method Viewed' then 1 else 0 end) as l_start,
    max(case when event_type in ({LIST_DONE}) then 1 else 0 end) as l_done
  from ev group by user_id, d
),
flows as (
  select user_id,
    sum(o_start) + sum(l_start) as flow_starts,
    sum(case when o_start = 1 and o_done = 0 then 1 else 0 end)
      + sum(case when l_start = 1 and l_done = 0 then 1 else 0 end) as flow_abandons
  from flow_days group by user_id
),
device as (
  select user_id,
    avg(case when lower(os_name) in ('ios', 'android') then 1.0 else 0.0 end) as mobile_share
  from ev where event_type = 'Viewed' group by user_id
),
outreach as (
  select o.user_id,
    count(distinct case when r.status not in ('draft', 'canceled') then r.id end) as outreach_created,
    count(distinct case when r.status not in ('draft', 'canceled') then date_trunc('week', r.createdAt) end)
      as outreach_weeks,
    percentile(case when r.outreach_type = 'p2p' and r.status not in ('draft', 'canceled')
        then length(r.message) end, 0.5) as text_length_med,
    percentile(case when r.outreach_type = 'p2p' and r.status not in ('draft', 'canceled')
        then r.text_count end, 0.5) as text_audience_med
  from orgs o
  join pop p on p.user_id = o.user_id
  join goodparty_data_catalog.airbyte_source.gp_api_db_outreach r
    on r.organization_slug = o.organization_slug and r.createdAt >= p.su and r.createdAt < p.we
  group by o.user_id
),
lists as (
  select o.user_id, count(distinct v.id) as lists_built,
    avg(case when v.recommended_intent is not null then 1.0 else 0.0 end) as lists_recommended_share
  from orgs o
  join pop p on p.user_id = o.user_id
  join goodparty_data_catalog.airbyte_source.gp_api_db_voter_file_filter v
    on v.organization_slug = o.organization_slug and v.created_at >= p.su and v.created_at < p.we
  group by o.user_id
),
chat as (
  select p.user_id, count(distinct cc.id) as chat_conversations,
    count(case when m.role = 'user' then 1 end) as chat_user_msgs,
    count(distinct case when m.role = 'user' then date(m.created_at) end) as chat_days
  from pop p
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_conversation cc
    on cc.owner_user_id = p.user_id and cc.scope = 'campaign_assistant'
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_message m
    on m.conversation_id = cc.id and m.created_at >= p.su and m.created_at < p.we
  group by p.user_id
),
site as (
  select o.user_id,
    max(coalesce(json_array_length(get_json_object(w.content, '$.about.issues')), 0)) as website_issue_count
  from orgs o join goodparty_data_catalog.airbyte_source.gp_api_db_website w on w.campaign_id = o.campaign_id
  group by o.user_id
)
select p.user_id, p.su as signed_up_at, p.election_date, p.we as window_end,
  datediff(date(p.we), date(p.su)) as days_on_win,
  datediff(p.election_date, date(timestamp'{run_ts}')) as days_to_election_at_run,
  date_format(p.su, 'yyyy-MM') as signup_month,
  coalesce(onb.onboarded, 0) as onboarded,
  coalesce(b.is_activated, false) as activated,
  b.first_campaign_sent_at,
  j.is_pro, j.has_run_before, j.election_year_count, j.candidacy_office_level_normalized as office_level,
  j.candidacy_is_incumbent as is_incumbent,
  j.outreach_sent_count, j.outreach_recipient_count, j.outreach_channel_count,
  j.outreach_texting_count, j.outreach_robocall_count, j.outreach_social_count,
  j.outreach_door_knocking_count, j.outreach_phone_banking_count,
  case when j.outreach_first_sent_at < p.we then datediff(p.election_date, date(j.outreach_first_sent_at))
       end as first_send_days_before_election,
  ef.* except (user_id), os.onboarding_minutes, fl.flow_starts, fl.flow_abandons, dv.mobile_share,
  orr.outreach_created, orr.outreach_weeks, orr.text_length_med, orr.text_audience_med,
  li.lists_built, li.lists_recommended_share,
  ch.chat_conversations, ch.chat_user_msgs, ch.chat_days,
  st.website_issue_count
from pop p
join goodparty_data_catalog.mart_analytics.user_journey j on j.user_id = p.user_id
left join goodparty_data_catalog.mart_analytics.users_win_base b on b.user_id = p.user_id
left join onb on onb.user_id = p.user_id
left join ev_feat ef on ef.user_id = p.user_id
left join onb_speed os on os.user_id = p.user_id
left join flows fl on fl.user_id = p.user_id
left join device dv on dv.user_id = p.user_id
left join outreach orr on orr.user_id = p.user_id
left join lists li on li.user_id = p.user_id
left join chat ch on ch.user_id = p.user_id
left join site st on st.user_id = p.user_id
"""


TOPIC_MODELS = ("databricks-claude-opus-5-5", "databricks-gpt-6-sol")
TOPIC_PROMPT = (
    "Classify this message a political candidate sent to an AI campaign assistant into exactly one "
    "category. Answer with the category text only. Categories: " + "; ".join(TOPICS) + ". Use "
    "'how to use the app' only when they ask how the software itself works; use 'complaint or "
    "frustration' only when the main point is a complaint. Message: "
)


def chat_topics_sql(run_ts: str) -> str:
    # Three classifiers, majority vote; a three-way split counts as 'other'. A single classifier
    # agreed with a second only 64% of the time (validate_topics.py), worst on complaints and how-to.
    # Classified in the warehouse; only per-user label counts leave it, never message text.
    labels = ", ".join(f"'{t}'" for t in TOPICS)
    prompt = TOPIC_PROMPT.replace("'", "''")
    llm = ",\n    ".join(
        f"lower(trim(ai_query('{m}', concat('{prompt}', content)))) as m{i}"
        for i, m in enumerate(TOPIC_MODELS)
    )
    return f"""
with {population_sql(run_ts)},
msgs as (
  select p.user_id, m.content
  from pop p
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_conversation cc
    on cc.owner_user_id = p.user_id and cc.scope = 'campaign_assistant'
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_message m
    on m.conversation_id = cc.id and m.role = 'user' and m.created_at >= p.su and m.created_at < p.we
  where length(trim(m.content)) > 0
),
votes as (
  select user_id, ai_classify(content, array({labels})) as c0,
    {llm}
  from msgs
),
clean as (
  select user_id, c0,
    case when regexp_replace(m0, '[^a-z ,]', '') in ({labels}) then regexp_replace(m0, '[^a-z ,]', '') end as c1,
    case when regexp_replace(m1, '[^a-z ,]', '') in ({labels}) then regexp_replace(m1, '[^a-z ,]', '') end as c2
  from votes
)
select user_id,
  case when c0 = c1 or c0 = c2 then c0 when c1 = c2 then c1 else 'other' end as topic,
  case when c0 = c1 and c1 = c2 then 3 when c0 = c1 or c0 = c2 or c1 = c2 then 2 else 1 end as agreement,
  count(*) as n
from clean group by 1, 2, 3
"""


def area_days_sql(run_ts: str) -> str:
    # Distinct active days per product area: the unit for each user's activity mix and journey order.
    # Onboarding steps, paywall and compliance screens, and the plan (now the landing page) are left
    # out so the mix reflects chosen work, not where everyone lands.
    return f"""
with {population_sql(run_ts)},
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
  join goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
    on try_cast(e.user_id as bigint) = p.user_id and e.event_time >= p.su and e.event_time < p.we
  left join goodparty_data_catalog.dbt.int__amplitude_event_catalog cat on cat.event_type = e.event_type
  where e.event_time >= timestamp'{SIGNUP_FLOOR} 00:00:00' and not coalesce(cat.is_machine_emitted, false)
),
db as (
  select o.user_id, date(r.createdAt) as d, 'outreach' as area, 1 as act
  from orgs o join pop p on p.user_id = o.user_id
  join goodparty_data_catalog.airbyte_source.gp_api_db_outreach r
    on r.organization_slug = o.organization_slug and r.createdAt >= p.su and r.createdAt < p.we
  union all
  select o.user_id, date(v.created_at), 'voter_data', 1
  from orgs o join pop p on p.user_id = o.user_id
  join goodparty_data_catalog.airbyte_source.gp_api_db_voter_file_filter v
    on v.organization_slug = o.organization_slug and v.created_at >= p.su and v.created_at < p.we
  union all
  select p.user_id, date(m.created_at), 'ai', 1
  from pop p
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_conversation cc
    on cc.owner_user_id = p.user_id and cc.scope = 'campaign_assistant'
  join goodparty_data_catalog.airbyte_source.gp_api_db_chat_message m
    on m.conversation_id = cc.id and m.role = 'user' and m.created_at >= p.su and m.created_at < p.we
),
allx as (select user_id, d, area, act from ev where area is not null union all select user_id, d, area, act from db)
select user_id, area, count(distinct d) as days, min(d) as first_day,
  count(distinct case when act = 1 then d end) as action_days, min(case when act = 1 then d end) as first_action_day
from allx group by 1, 2
"""


def main() -> None:
    OUT.mkdir(exist_ok=True)
    if "--topics-only" in sys.argv:
        # Reuse the pinned run timestamp so topics cover exactly the working-set population.
        import pandas as pd

        run_ts = pd.read_parquet(OUT / "working_set.parquet", columns=["run_ts"])["run_ts"].iloc[0]
    else:
        run_ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        ws = dbc.run_query(working_set_sql(run_ts))
        ws["run_ts"] = run_ts
        ws.to_parquet(OUT / "working_set.parquet", index=False)
        print(f"working set: {len(ws):,} users")
    print(f"run timestamp (UTC): {run_ts}")
    if "--topics-only" not in sys.argv:
        ad = dbc.run_query(area_days_sql(run_ts))
        ad["run_ts"] = run_ts
        ad.to_parquet(OUT / "area_days.parquet", index=False)
        print(f"area days: {len(ad):,} user-area rows")
    if "--skip-topics" not in sys.argv:
        topics = dbc.run_query(chat_topics_sql(run_ts))
        topics["run_ts"] = run_ts
        topics.to_parquet(OUT / "chat_topics.parquet", index=False)
        print(f"chat topics: {topics['n'].sum():,} messages across {topics['user_id'].nunique():,} users")


if __name__ == "__main__":
    main()
