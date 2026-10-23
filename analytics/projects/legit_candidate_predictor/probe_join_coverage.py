"""DATA-2239 Area 2 step 0b: measure the join rate of every candidate feature source.

A source is only worth building if it actually reaches the predictor population. Each probe
re-derives the working-set population in SQL (same filter as build_working_set.py) and reports
coverage, not row dumps. Aggregate output only: no individual voter or user records leave the
warehouse.

Usage:
    uv run --with pyarrow python projects/legit_candidate_predictor/probe_join_coverage.py [probe ...]

Probes: segment, campaign_history, user_table, prospects, race_priors, l2_name (l2_name is the
expensive one; run it alone).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lib"))
from databricks_conn import run_query

# Population = build_working_set.py's, minus the label/feature joins.
POP = """
pop as (
  select u.user_id,
         u.campaign_id,
         cast(u.user_created_at as date) as signup_date,
         u.campaign_state,
         u.user_first_name,
         u.user_last_name,
         cast(u.ballotready_position_id as bigint) as br_position_id,
         c.general_election_date,
         u.hubspot_id
  from goodparty_data_catalog.mart_analytics.users_win_candidacy u
  join goodparty_data_catalog.mart_civics.candidacy c
    on u.campaign_id = c.product_campaign_id
   and u.election_date = c.general_election_date
  where u.is_latest_version
    and not u.is_demo
    and c.general_election_date between date'2025-01-01' and date'2050-01-01'
    and c.general_election_date < current_date()
)
"""

PROBES: dict[str, str] = {}

# --- Segment web behavior: is pre-signup behavior reachable, and for how many? ---
PROBES["segment"] = f"""
with {POP},
ident as (
  select user_id, min(anonymous_id) as any_anon, count(distinct anonymous_id) as n_anon,
         min(timestamp) as first_ident, max(sign_up_method) as sign_up_method
  from goodparty_data_catalog.dbt.stg_segment_storage_source__web_app_identifies
  where user_id is not null
  group by user_id
),
pages_ident as (
  select p.user_id, min(p.timestamp) as first_page, count(*) as n_pages
  from goodparty_data_catalog.dbt.stg_segment_storage_source__web_app_pages p
  where p.user_id is not null
  group by p.user_id
)
select
  count(*)                                                              as pop_rows,
  sum(case when i.user_id is not null then 1 else 0 end)                as in_identifies,
  sum(case when pg.user_id is not null then 1 else 0 end)               as in_pages_by_user_id,
  sum(case when i.n_anon > 1 then 1 else 0 end)                         as multi_anon_users,
  sum(case when i.sign_up_method is not null then 1 else 0 end)         as has_signup_method,
  sum(case when pg.first_page < cast(p.signup_date as timestamp) then 1 else 0 end) as first_page_before_signup,
  min(p.signup_date)                                                    as pop_first_signup,
  max(p.signup_date)                                                    as pop_last_signup,
  min(i.first_ident)                                                    as ident_first_seen,
  sum(case when p.signup_date >= date'2026-01-01' then 1 else 0 end)     as pop_2026,
  sum(case when p.signup_date >= date'2026-01-01' and i.user_id is not null then 1 else 0 end) as pop_2026_in_identifies
from pop p
left join ident i on i.user_id = cast(p.user_id as string)
left join pages_ident pg on pg.user_id = cast(p.user_id as string)
"""

# --- Anonymous pre-signup sessions via anonymous_id bridge ---
PROBES["segment_anon"] = f"""
with {POP},
anon as (
  select cast(user_id as string) as user_id, anonymous_id, min(timestamp) as first_ident_ts
  from goodparty_data_catalog.dbt.stg_segment_storage_source__web_app_identifies
  where user_id is not null and anonymous_id is not null
  group by 1, 2
),
pre as (
  select a.user_id,
         count(*)                                        as n_pre_pages,
         count(distinct pg.context_actions_amplitude_session_id) as n_pre_sessions,
         min(pg.timestamp)                               as first_touch_ts,
         max(pg.utm_source_first)                        as utm_source_first,
         max(pg.referrer)                                as first_referrer
  from anon a
  join goodparty_data_catalog.dbt.stg_segment_storage_source__web_app_pages pg
    on pg.anonymous_id = a.anonymous_id
  join pop p2 on p2.user_id = cast(a.user_id as bigint)
  where pg.timestamp < cast(p2.signup_date as timestamp)
  group by a.user_id
)
select count(*) as pop_rows,
       sum(case when pre.user_id is not null then 1 else 0 end) as has_pre_signup_pages,
       sum(case when p.signup_date >= date'2026-01-01' then 1 else 0 end) as pop_2026,
       sum(case when p.signup_date >= date'2026-01-01' and pre.user_id is not null then 1 else 0 end)
         as pop_2026_has_pre,
       avg(pre.n_pre_pages)     as avg_pre_pages,
       avg(pre.n_pre_sessions)  as avg_pre_sessions,
       sum(case when pre.utm_source_first is not null then 1 else 0 end) as has_utm_source,
       sum(case when pre.first_referrer is not null and pre.first_referrer <> '' then 1 else 0 end) as has_referrer
from pop p
left join pre on pre.user_id = cast(p.user_id as string)
"""

# --- Campaign details blob: is as-of-signup reconstruction possible from Airbyte history? ---
PROBES["campaign_history"] = f"""
with {POP},
hist as (
  select cast(id as string) as campaign_id,
         count(*)                                  as n_versions,
         count(distinct _airbyte_extracted_at)      as n_extracts,
         min(_airbyte_extracted_at)                 as first_extract,
         max(_airbyte_extracted_at)                 as last_extract
  from goodparty_data_catalog.dbt.stg_airbyte_internal__raw_gp_api_db_campaign
  group by 1
)
select count(*)                                                     as pop_rows,
       sum(case when h.campaign_id is not null then 1 else 0 end)    as in_raw_history,
       avg(h.n_versions)                                             as avg_versions,
       percentile(h.n_versions, 0.5)                                 as median_versions,
       sum(case when h.n_versions > 1 then 1 else 0 end)              as multi_version,
       sum(case when cast(h.first_extract as date) <= p.signup_date + interval 7 days
                then 1 else 0 end)                                   as first_extract_within_7d_of_signup,
       min(h.first_extract)                                          as earliest_extract_overall
from pop p
left join hist h on h.campaign_id = cast(p.campaign_id as string)
"""

# --- Details blob key inventory by era (which keys exist at all, and when) ---
PROBES["details_keys"] = f"""
with {POP},
c as (
  select cast(cp.id as string) as campaign_id, cp.details
  from goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_campaign cp
),
j as (
  select p.signup_date, c.details
  from pop p join c on c.campaign_id = cast(p.campaign_id as string)
),
ex as (
  select case when signup_date < date'2025-06-01' then '2025H1'
              when signup_date < date'2026-01-01' then '2025H2'
              else '2026' end as era,
         explode(from_json(details, 'map<string,string>')) as (k, v)
  from j
)
select k, era, count(*) as n, sum(case when v is not null and v <> '' then 1 else 0 end) as n_nonempty
from ex group by 1, 2 order by k, era
"""

# --- User table leftovers ---
PROBES["user_table"] = f"""
with {POP}
select count(*)                                                             as pop_rows,
       sum(case when u.id is not null then 1 else 0 end)                     as in_user_table,
       sum(case when u.phone is not null and u.phone <> '' then 1 else 0 end) as has_phone,
       sum(case when u.avatar is not null and u.avatar <> '' then 1 else 0 end) as has_avatar,
       sum(case when u.has_password then 1 else 0 end)                        as has_password,
       sum(case when u.meta_data is not null then 1 else 0 end)               as has_meta_data,
       sum(case when u.zip is not null and u.zip <> '' then 1 else 0 end)     as has_zip
from pop p
left join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user u
  on cast(u.id as string) = cast(p.user_id as string)
"""

# --- Sales/CRM provenance (the no-browser-presence hypothesis) ---
PROBES["prospects"] = f"""
with {POP}
select count(*)                                                          as pop_rows,
       sum(case when pr.gp_user_id is not null then 1 else 0 end)         as in_prospects,
       sum(case when p.hubspot_id is not null then 1 else 0 end)          as has_hubspot_id,
       sum(case when pr.first_sales_touch_at is not null then 1 else 0 end) as has_sales_touch,
       sum(case when pr.first_sales_touch_at is not null
                 and cast(pr.first_sales_touch_at as date) < p.signup_date then 1 else 0 end)
                                                                          as sales_touch_before_signup,
       sum(case when pr.is_self_filer_lead then 1 else 0 end)             as is_self_filer_lead,
       sum(case when pr.lead_status is not null then 1 else 0 end)        as has_lead_status
from pop p
left join goodparty_data_catalog.mart_analytics.prospects pr
  on cast(pr.gp_user_id as string) = cast(p.user_id as string)
"""

# --- Race-context priors ---
PROBES["race_priors"] = f"""
with {POP}
select count(*)                                                        as pop_rows,
       count(distinct p.br_position_id)                                 as distinct_positions,
       sum(case when f.database_id is not null then 1 else 0 end)        as in_br_position,
       sum(case when f.seats is not null then 1 else 0 end)             as has_seats,
       sum(case when f.salary is not null and f.salary <> '' then 1 else 0 end) as has_salary,
       sum(case when f.maximum_filing_fee is not null then 1 else 0 end) as has_max_filing_fee,
       sum(case when f.minimum_age is not null then 1 else 0 end)        as has_minimum_age,
       sum(case when f.filing_requirements is not null and f.filing_requirements <> '' then 1 else 0 end)
                                                                        as has_filing_requirements,
       sum(case when f.must_be_registered_voter is not null then 1 else 0 end) as has_must_be_registered,
       sum(case when f.election_frequencies is not null then 1 else 0 end) as has_election_frequencies,
       sum(case when f.has_primary is not null then 1 else 0 end)        as has_has_primary,
       sum(case when f.is_partisan is not null then 1 else 0 end)        as has_is_partisan,
       sum(case when f.tier is not null then 1 else 0 end)               as has_tier,
       sum(case when oh.n_holders > 0 then 1 else 0 end)                as position_has_office_holder,
       sum(case when np.database_id is not null then 1 else 0 end)       as in_normalized_position
from pop p
left join goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_api_position f
  on cast(f.database_id as string) = cast(p.br_position_id as string)
left join (
  select cast(br_position_id as string) as position_id, count(*) as n_holders
  from goodparty_data_catalog.dbt.stg_airbyte_source__ballotready_s3_office_holders_v3
  group by 1
) oh on oh.position_id = cast(p.br_position_id as string)
left join goodparty_data_catalog.dbt.int__ballotready_normalized_position np
  on cast(np.database_id as string) = cast(p.br_position_id as string)
"""

# --- L2 person name match (expensive: nationwide voter file scan) ---
PROBES["l2_name"] = f"""
with {POP},
nm as (
  select user_id, signup_date, campaign_state,
         upper(trim(user_first_name)) as fn, upper(trim(user_last_name)) as ln
  from pop
  where user_first_name is not null and user_last_name is not null and campaign_state is not null
),
matched as (
  select nm.user_id,
         count(v.LALVOTERID)                                          as n_voters,
         max(case when v.Voters_Active = 'A' then 1 else 0 end)         as any_active,
         max(case when v.Voters_CalculatedRegDate is not null then 1 else 0 end) as any_regdate
  from nm
  left join goodparty_data_catalog.dbt.int__l2_nationwide_uniform v
    on v.state_postal_code = nm.campaign_state
   and upper(trim(v.Voters_FirstName)) = nm.fn
   and upper(trim(v.Voters_LastName)) = nm.ln
  group by nm.user_id
)
select count(*)                                                  as candidates_with_name_and_state,
       sum(case when n_voters >= 1 then 1 else 0 end)              as any_match,
       sum(case when n_voters = 1 then 1 else 0 end)               as unique_match,
       sum(case when n_voters between 2 and 5 then 1 else 0 end)   as match_2_5,
       sum(case when n_voters > 5 then 1 else 0 end)               as match_gt5,
       sum(any_active)                                            as any_active_match,
       sum(any_regdate)                                           as any_regdate_match
from matched
"""


# --- Amplitude device_id bridge: pre-signup anonymous behavior, the historical-depth source ---
PROBES["amplitude_device"] = f"""
with {POP},
dev as (
  select try_cast(e.user_id as bigint) as user_id, e.device_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
  join pop p on p.user_id = try_cast(e.user_id as bigint)
  where e.device_id is not null
    and cast(e.event_time as date) between date_add(p.signup_date, -1) and date_add(p.signup_date, 30)
  group by 1, 2
),
pre as (
  select d.user_id,
         count(*)                          as n_pre_events,
         count(distinct e.session_id)       as n_pre_sessions,
         min(e.event_time)                  as first_touch,
         max(case when e.region is not null then 1 else 0 end) as has_geo_pre
  from dev d
  join pop p2 on p2.user_id = d.user_id
  join goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
    on e.device_id = d.device_id
  where e.event_time < cast(p2.signup_date as timestamp)
    and (e.user_id is null or try_cast(e.user_id as bigint) <> d.user_id)
  group by d.user_id
)
select count(*)                                                       as pop_rows,
       count(distinct d.user_id)                                       as users_with_device_id,
       count(distinct pre.user_id)                                     as users_with_pre_signup_events,
       sum(case when p.signup_date >= date'2026-01-01' then 1 else 0 end) as pop_2026,
       avg(pre.n_pre_events)                                           as avg_pre_events,
       avg(pre.n_pre_sessions)                                         as avg_pre_sessions,
       avg(datediff(cast(p.signup_date as timestamp), pre.first_touch)) as avg_first_touch_lag_days
from pop p
left join (select distinct user_id from dev) d on d.user_id = p.user_id
left join pre on pre.user_id = p.user_id
"""

# --- Is the raw campaign table a real history or just Airbyte generations? ---
PROBES["campaign_gen"] = """
select _airbyte_generation_id,
       count(*)                        as n_rows,
       count(distinct id)              as n_campaigns,
       min(_airbyte_extracted_at)      as first_extract,
       max(_airbyte_extracted_at)      as last_extract
from goodparty_data_catalog.dbt.stg_airbyte_internal__raw_gp_api_db_campaign
group by 1 order by 1
"""

# --- User meta_data blob: what keys are in it, and are they signup-tier? ---
PROBES["user_meta"] = f"""
with {POP},
u as (
  select p.signup_date, us.meta_data
  from pop p
  join goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_user us
    on cast(us.id as string) = cast(p.user_id as string)
),
ex as (
  select case when signup_date < date'2025-01-01' then 'pre2025'
              when signup_date < date'2026-01-01' then '2025'
              else '2026' end as era,
         explode(from_json(meta_data, 'map<string,string>')) as (k, v)
  from u
)
select k, era, count(*) as n, sum(case when v is not null and v <> '' and v <> 'null' then 1 else 0 end) as n_nonempty
from ex group by 1, 2 order by k, era
"""

# --- HubSpot join keys: users_win_candidacy.hubspot_id is NOT dbt hubspot `id`. Find what works. ---
PROBES["hubspot_keys"] = f"""
with {POP},
c as (
  select cast(id as string)                as c_id,
         cast(hs_object_id as string)      as c_object_id,
         cast(goodparty_user_id as string) as c_gp_user_id,
         lower(trim(email))                as c_email
  from goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_contacts
),
u as (
  select p.user_id, cast(p.hubspot_id as string) as hubspot_id, lower(trim(uw.user_email)) as email
  from pop p
  join goodparty_data_catalog.mart_analytics.users_win_candidacy uw
    on uw.campaign_id = p.campaign_id and uw.is_latest_version
)
select (select count(*) from u)                                                        as pop_rows,
       (select count(*) from u join c on c.c_object_id = u.hubspot_id)                 as via_hs_object_id,
       (select count(*) from u join c on c.c_gp_user_id = cast(u.user_id as string))   as via_goodparty_user_id,
       (select count(*) from u join c on c.c_email = u.email)                          as via_email,
       (select count(*) from u join c on c.c_id = u.hubspot_id)                        as via_id
"""

# --- HubSpot property fill: pre-signup attribution (pass-B legal) vs label-adjacent fields ---
PROBES["hubspot_fields"] = f"""
with {POP},
c as (
  select cast(goodparty_user_id as string) as gp_user_id,
         hs_analytics_first_timestamp, hs_analytics_first_visit_timestamp,
         hs_analytics_first_referrer, hs_analytics_first_url, hs_analytics_source,
         hs_analytics_first_touch_converting_campaign, hs_analytics_num_visits,
         channel_group__first_conversion_source, conversion_source, lead_source, utm_source,
         candidate_experience_level, has_ein, birth_date, website, facebook_url, twitter_handle,
         linkedin_url, instagram_handle, number_of_opponents, is_incumbent, population,
         is_filed_candidate, is_confirmed_candidate, verified_candidate_status,
         candidate_id_source, candidate_id_tier, br_position_id
  from goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_contacts
  where goodparty_user_id is not null
)
select case when p.signup_date < date'2025-01-01' then 'pre2025'
            when p.signup_date < date'2026-01-01' then '2025' else '2026' end as era,
       count(*) as n,
       sum(case when c.gp_user_id is not null then 1 else 0 end)                    as joined,
       sum(case when c.hs_analytics_first_visit_timestamp is not null then 1 else 0 end) as has_first_visit_ts,
       sum(case when c.hs_analytics_first_referrer is not null then 1 else 0 end)    as has_first_referrer,
       sum(case when c.hs_analytics_first_url is not null then 1 else 0 end)         as has_first_url,
       sum(case when c.hs_analytics_source is not null then 1 else 0 end)            as has_analytics_source,
       sum(case when c.hs_analytics_num_visits is not null then 1 else 0 end)        as has_num_visits,
       sum(case when c.channel_group__first_conversion_source is not null then 1 else 0 end) as has_channel_group,
       sum(case when c.utm_source is not null then 1 else 0 end)                     as has_utm_source,
       sum(case when c.candidate_experience_level is not null then 1 else 0 end)     as has_experience_level,
       sum(case when c.has_ein is not null then 1 else 0 end)                        as has_ein_field,
       sum(case when c.birth_date is not null then 1 else 0 end)                     as has_birth_date,
       sum(case when c.website is not null then 1 else 0 end)                        as has_website,
       sum(case when c.facebook_url is not null then 1 else 0 end)                   as has_facebook,
       sum(case when c.linkedin_url is not null then 1 else 0 end)                   as has_linkedin,
       sum(case when c.number_of_opponents is not null then 1 else 0 end)            as has_n_opponents,
       sum(case when c.is_incumbent is not null then 1 else 0 end)                   as has_is_incumbent,
       sum(case when c.population is not null then 1 else 0 end)                     as has_population,
       sum(case when c.is_filed_candidate is not null then 1 else 0 end)             as LEAK_is_filed,
       sum(case when c.verified_candidate_status is not null then 1 else 0 end)       as LEAK_verified_status,
       sum(case when c.br_position_id is not null then 1 else 0 end)                 as LEAK_br_position_id
from pop p
left join c on c.gp_user_id = cast(p.user_id as string)
group by 1 order by 1
"""

# --- Same fill rates, but on the email key (99.6% join vs 28% on goodparty_user_id) ---
PROBES["hubspot_fields_email"] = f"""
with {POP},
u as (
  select p.user_id, p.signup_date, lower(trim(uw.user_email)) as email
  from pop p
  join goodparty_data_catalog.mart_analytics.users_win_candidacy uw
    on uw.campaign_id = p.campaign_id and uw.is_latest_version
),
c as (
  select lower(trim(email)) as email,
         max(hs_analytics_first_visit_timestamp) as first_visit,
         max(hs_analytics_first_referrer)        as first_referrer,
         max(hs_analytics_source)                as analytics_source,
         max(hs_analytics_num_visits)            as num_visits,
         max(channel_group__first_conversion_source) as channel_group,
         max(conversion_source)                  as conversion_source,
         max(lead_source)                        as lead_source,
         max(candidate_experience_level)         as experience_level,
         max(has_ein)                            as has_ein,
         max(birth_date)                         as birth_date,
         max(website)                            as website,
         max(facebook_url)                       as facebook_url,
         max(linkedin_url)                       as linkedin_url,
         max(number_of_opponents)                as number_of_opponents,
         max(is_incumbent)                       as is_incumbent,
         max(population)                         as population,
         max(verified_candidate_status)          as verified_status
  from goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_contacts
  where email is not null
  group by 1
)
select case when u.signup_date < date'2025-01-01' then 'pre2025'
            when u.signup_date < date'2026-01-01' then '2025' else '2026' end as era,
       count(*) as n,
       sum(case when c.email is not null then 1 else 0 end)                     as joined,
       sum(case when c.first_visit is not null then 1 else 0 end)               as has_first_visit,
       sum(case when cast(c.first_visit as date) < u.signup_date then 1 else 0 end) as first_visit_pre_signup,
       sum(case when c.first_referrer is not null then 1 else 0 end)            as has_first_referrer,
       sum(case when c.analytics_source is not null then 1 else 0 end)          as has_analytics_source,
       sum(case when c.channel_group is not null then 1 else 0 end)             as has_channel_group,
       sum(case when c.conversion_source is not null then 1 else 0 end)         as has_conversion_source,
       sum(case when c.lead_source is not null then 1 else 0 end)               as has_lead_source,
       sum(case when c.experience_level is not null then 1 else 0 end)          as has_experience_level,
       sum(case when c.has_ein is not null then 1 else 0 end)                   as has_ein_field,
       sum(case when c.birth_date is not null then 1 else 0 end)                as has_birth_date,
       sum(case when c.website is not null then 1 else 0 end)                   as has_website,
       sum(case when c.facebook_url is not null then 1 else 0 end)              as has_facebook,
       sum(case when c.linkedin_url is not null then 1 else 0 end)              as has_linkedin,
       sum(case when c.number_of_opponents is not null then 1 else 0 end)       as has_n_opponents,
       sum(case when c.is_incumbent is not null then 1 else 0 end)              as has_is_incumbent,
       sum(case when c.population is not null then 1 else 0 end)                as has_population,
       sum(case when c.verified_status is not null then 1 else 0 end)           as LEAK_verified_status
from u
left join c on c.email = u.email
group by 1 order by 1
"""

# --- First-touch timing: is the HubSpot first visit actually before signup (PIT-safe)? ---
PROBES["hubspot_firsttouch"] = f"""
with {POP},
c as (
  select cast(goodparty_user_id as string) as gp_user_id,
         hs_analytics_first_visit_timestamp as first_visit,
         hs_analytics_first_timestamp       as first_ts,
         hs_analytics_source                as src
  from goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_contacts
  where goodparty_user_id is not null
)
select count(*)                                                                   as joined_rows,
       sum(case when c.first_visit is not null then 1 else 0 end)                   as has_first_visit,
       sum(case when cast(c.first_visit as date) < p.signup_date then 1 else 0 end) as first_visit_before_signup,
       sum(case when cast(c.first_visit as date) = p.signup_date then 1 else 0 end) as first_visit_on_signup,
       sum(case when cast(c.first_visit as date) > p.signup_date then 1 else 0 end) as first_visit_after_signup,
       percentile(datediff(p.signup_date, cast(c.first_visit as date)), array(0.25, 0.5, 0.75, 0.95))
                                                                                    as lag_days_p25_50_75_95,
       min(cast(c.first_visit as date))                                             as earliest_first_visit
from pop p
join c on c.gp_user_id = cast(p.user_id as string)
"""

# --- Does the L2 match itself discriminate the label? The "is this a real human" question. ---
PROBES["l2_match_signal"] = f"""
with {POP},
corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
lab as (
  select p.user_id, p.campaign_state,
         upper(trim(p.user_first_name)) as fn, upper(trim(p.user_last_name)) as ln,
         coalesce(k.corroborated, 0) as corroborated
  from pop p
  join goodparty_data_catalog.mart_civics.candidacy c
    on c.product_campaign_id = p.campaign_id and c.general_election_date = p.general_election_date
  left join corrob k on k.gp_candidacy_id = c.gp_candidacy_id
),
matched as (
  select lab.user_id, lab.corroborated,
         count(v.LALVOTERID) as n_voters,
         max(case when v.Voters_Active = 'A' then 1 else 0 end) as any_active
  from lab
  left join goodparty_data_catalog.dbt.int__l2_nationwide_uniform v
    on v.state_postal_code = lab.campaign_state
   and upper(trim(v.Voters_FirstName)) = lab.fn
   and upper(trim(v.Voters_LastName)) = lab.ln
  where lab.fn is not null and lab.ln is not null and lab.campaign_state is not null
  group by lab.user_id, lab.corroborated
)
select case when n_voters = 0 then '0_no_match'
            when n_voters = 1 then '1_unique'
            when n_voters <= 5 then '2_ambiguous_2_5'
            else '3_ambiguous_gt5' end as match_bucket,
       count(*)                as n,
       avg(corroborated)       as corroboration_rate,
       avg(any_active)         as share_with_active_voter
from matched group by 1 order by 1
"""

# --- Name plausibility: does the name exist in the 218M-row voter file at all? ---
PROBES["name_plausibility"] = f"""
with {POP},
corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
lab as (
  select p.user_id,
         upper(trim(p.user_first_name)) as fn, upper(trim(p.user_last_name)) as ln,
         coalesce(k.corroborated, 0) as corroborated
  from pop p
  join goodparty_data_catalog.mart_civics.candidacy c
    on c.product_campaign_id = p.campaign_id and c.general_election_date = p.general_election_date
  left join corrob k on k.gp_candidacy_id = c.gp_candidacy_id
  where p.user_first_name is not null and p.user_last_name is not null
),
fn_freq as (
  select upper(trim(Voters_FirstName)) as fn, count(*) as n_fn
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_FirstName is not null
  group by 1
),
ln_freq as (
  select upper(trim(Voters_LastName)) as ln, count(*) as n_ln
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_LastName is not null
  group by 1
),
j as (
  select lab.*, coalesce(f.n_fn, 0) as n_fn, coalesce(l.n_ln, 0) as n_ln
  from lab left join fn_freq f on f.fn = lab.fn left join ln_freq l on l.ln = lab.ln
)
select case when n_fn = 0 and n_ln = 0 then '0_neither_name_exists'
            when n_fn = 0 then '1_first_name_unknown'
            when n_ln = 0 then '2_last_name_unknown'
            when n_fn < 50 or n_ln < 50 then '3_both_rare'
            else '4_both_common' end as plausibility_bucket,
       count(*)          as n,
       avg(corroborated) as corroboration_rate,
       percentile(n_fn, 0.5) as median_first_name_freq,
       percentile(n_ln, 0.5) as median_last_name_freq
from j group by 1 order by 1
"""

# --- Name plausibility, part 2: exact-pair rarity and placeholder patterns ---
PROBES["name_patterns"] = f"""
with {POP},
corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
lab as (
  select p.user_id,
         upper(trim(p.user_first_name)) as fn, upper(trim(p.user_last_name)) as ln,
         coalesce(k.corroborated, 0) as corroborated
  from pop p
  join goodparty_data_catalog.mart_civics.candidacy c
    on c.product_campaign_id = p.campaign_id and c.general_election_date = p.general_election_date
  left join corrob k on k.gp_candidacy_id = c.gp_candidacy_id
  where p.user_first_name is not null and p.user_last_name is not null
),
pair_freq as (
  select upper(trim(Voters_FirstName)) as fn, upper(trim(Voters_LastName)) as ln, count(*) as n_pair
  from goodparty_data_catalog.dbt.int__l2_nationwide_uniform
  where Voters_FirstName is not null and Voters_LastName is not null
  group by 1, 2
),
flags as (
  select lab.user_id, lab.corroborated, coalesce(pf.n_pair, 0) as n_pair,
         case when lab.fn = lab.ln then 1 else 0 end as fn_eq_ln,
         case when lab.fn rlike '(TEST|DEMO|SAMPLE|ASDF|QWERTY|ADMIN|USER|NONE|FAKE|XXX)'
                or lab.ln rlike '(TEST|DEMO|SAMPLE|ASDF|QWERTY|ADMIN|USER|NONE|FAKE|XXX)'
              then 1 else 0 end as placeholder_token,
         case when lab.fn rlike '(.)\\\\1\\\\1' or lab.ln rlike '(.)\\\\1\\\\1' then 1 else 0 end as triple_char,
         case when length(lab.fn) <= 2 or length(lab.ln) <= 2 then 1 else 0 end as very_short
  from lab left join pair_freq pf on pf.fn = lab.fn and pf.ln = lab.ln
)
select 'pair_freq_bucket' as dim,
       case when n_pair = 0 then '0_pair_never_seen'
            when n_pair = 1 then '1_pair_once'
            when n_pair <= 10 then '2_pair_2_10'
            else '3_pair_gt10' end as bucket,
       count(*) as n, avg(corroborated) as corroboration_rate
from flags group by 1, 2
union all
select 'fn_eq_ln', cast(fn_eq_ln as string), count(*), avg(corroborated) from flags group by 1, 2
union all
select 'placeholder_token', cast(placeholder_token as string), count(*), avg(corroborated) from flags group by 1, 2
union all
select 'triple_char', cast(triple_char as string), count(*), avg(corroborated) from flags group by 1, 2
union all
select 'very_short', cast(very_short as string), count(*), avg(corroborated) from flags group by 1, 2
order by dim, bucket
"""

# --- The "download the voter file and leave" archetype. Pass A / diagnostic, not a pass-B feature. ---
PROBES["download_archetype"] = f"""
with {POP},
corrob as (
  select gp_candidacy_id,
         max(case when br_candidacy_id is not null or ts_source_candidate_id is not null
                   or ddhq_candidate_id is not null then 1 else 0 end) as corroborated
  from goodparty_data_catalog.mart_civics.candidacy_stage
  group by gp_candidacy_id
),
lab as (
  select p.user_id, p.signup_date, coalesce(k.corroborated, 0) as corroborated
  from pop p
  join goodparty_data_catalog.mart_civics.candidacy c
    on c.product_campaign_id = p.campaign_id and c.general_election_date = p.general_election_date
  left join corrob k on k.gp_candidacy_id = c.gp_candidacy_id
),
-- only users who could have been observed: the download events start 2025-05-28
obs as (select * from lab where signup_date >= date'2025-05-28'),
ev as (
  select try_cast(e.user_id as bigint) as user_id, e.event_type, e.event_time
  from goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events e
  join obs o on o.user_id = try_cast(e.user_id as bigint)
  where e.session_id <> -1
),
agg as (
  select o.user_id, o.corroborated,
         max(case when ev.event_type = 'Download Voter File Success' then 1 else 0 end) as downloaded,
         max(case when ev.event_type = 'Download Voter File Success' then ev.event_time end) as last_dl,
         count(ev.event_type)                                          as n_events,
         count(distinct cast(ev.event_time as date))                    as n_active_days
  from obs o left join ev on ev.user_id = o.user_id
  group by o.user_id, o.corroborated
),
post as (
  select a.user_id,
         count(ev.event_type) as n_events_after_dl,
         count(distinct cast(ev.event_time as date)) as n_days_after_dl
  from agg a
  join ev on ev.user_id = a.user_id
  where a.last_dl is not null
    and ev.event_time > a.last_dl + interval 7 days
    and ev.event_type not rlike '(?i)(download|voter data|custom voter file)'
  group by a.user_id
)
select case when a.downloaded = 0 and a.n_events = 0 then '0_no_browser_events'
            when a.downloaded = 0 then '1_never_downloaded'
            when coalesce(p2.n_events_after_dl, 0) = 0 then '2_downloaded_and_left'
            else '3_downloaded_and_stayed' end as archetype,
       count(*)                    as n,
       avg(a.corroborated)         as corroboration_rate,
       avg(a.n_events)             as avg_events,
       avg(a.n_active_days)        as avg_active_days
from agg a
left join post p2 on p2.user_id = a.user_id
group by 1 order by 1
"""

if __name__ == "__main__":
    names = sys.argv[1:] or ["segment", "campaign_history", "user_table", "prospects", "race_priors"]
    for name in names:
        if name not in PROBES:
            print(f"!! unknown probe {name}; have {sorted(PROBES)}")
            continue
        print(f"\n===== {name} =====")
        try:
            df = run_query(PROBES[name])
            print(df.T.to_string() if len(df) == 1 else df.to_string())
        except Exception as exc:  # keep the remaining probes running
            print(f"FAILED: {type(exc).__name__}: {exc}")
