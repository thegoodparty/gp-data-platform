-- Power user definition v2, the definition for the 2026 cycle on: the action-day
-- bar for each election cycle at each distance from the election, 0 to 365
-- days out. One row per cycle per days_out. v1 (int__power_user_bar, dashboard
-- view-days) stays as the definition for its period.
--
-- A cycle is the calendar year of the election. Each cycle is measured on its
-- own completed races, because what Win offers changes between cycles (plan
-- tasks only tracked from late 2026, features retired mid-2026) and a 2026
-- candidate had a longer lead-in than a 2025 one. Within a cycle the method is
-- v1's: the 85th percentile of action days among users who had acted by that
-- point, rounded up, the bar never stricter further out, and days with fewer
-- than 500 actors carrying the last bar measured on enough.
--
-- A cycle whose completed races number fewer than 500 actors by election day
-- has no reliable bar of its own yet, so it borrows the latest measured cycle
-- before it. bar_source_cycle says which; the borrowed rows copy the source
-- cycle's counts too. Cycles before telemetry began have no rows.
{{ config(materialized="table") }}

{%- set min_actors = 500 -%}

with
    internal_users as (
        select k.user_id
        from {{ ref("int__user_resolved_keys") }} as k
        inner join
            {{ ref("int__civics_internal_persons") }} as i
            on i.gp_person_id = k.gp_person_id
    ),

    demo_only_users as (
        select user_id
        from {{ ref("users") }}
        where campaign_count > 0 and non_demo_campaign_count = 0
    ),

    -- Races start six months after telemetry, so a campaign that began before
    -- tracking does not read as idle.
    reference_users as (
        select
            c.user_id,
            c.candidacy_election_date as election_date,
            year(c.candidacy_election_date) as cycle
        from {{ ref("int__user_candidacy_profile") }} as c
        left anti join internal_users as i on i.user_id = c.user_id
        left anti join demo_only_users as d on d.user_id = c.user_id
        where
            c.has_candidacy
            and c.candidacy_election_date
            >= add_months(date('{{ var("product_telemetry_start") }}'), 6)
            and c.candidacy_election_date < current_date()
    ),

    action_days as (
        select r.user_id, r.election_date, d.activity_date
        from reference_users as r
        inner join
            {{ ref("int__user_action_days") }} as d
            on d.user_id = r.user_id
            and d.activity_date <= r.election_date
    ),

    days_out_spine as (select explode(sequence(0, 365)) as days_out),

    action_days_at as (
        select s.days_out, r.cycle, r.user_id, count(a.activity_date) as action_days
        from days_out_spine as s
        cross join reference_users as r
        left join
            action_days as a
            on a.user_id = r.user_id
            and a.activity_date <= date_sub(r.election_date, s.days_out)
        group by s.days_out, r.cycle, r.user_id
    ),

    raw_bar as (
        select
            cycle,
            days_out,
            count(*) as reference_user_count,
            count_if(action_days > 0) as actor_count,
            cast(
                ceil(
                    percentile(action_days, 0.85) filter (where action_days > 0)
                ) as int
            ) as raw_action_day_bar
        from action_days_at
        group by cycle, days_out
    ),

    -- Same two corrections as v1, within each cycle.
    smoothed as (
        select
            cycle,
            days_out,
            reference_user_count,
            actor_count,
            raw_action_day_bar,
            actor_count >= {{ min_actors }} as is_measured,
            min(
                case when actor_count >= {{ min_actors }} then raw_action_day_bar end
            ) over (
                partition by cycle
                order by days_out
                rows between unbounded preceding and current row
            ) as action_day_bar
        from raw_bar
    ),

    measured_cycles as (select cycle from smoothed where days_out = 0 and is_measured),

    last_cycle as (
        select max(year(candidacy_election_date)) as max_cycle
        from {{ ref("int__user_candidacy_profile") }}
        where has_candidacy
    ),

    -- Every cycle a candidacy can fall in, from telemetry's first year to the
    -- furthest election on file.
    cycle_spine as (
        select
            explode(
                sequence(year(date('{{ var("product_telemetry_start") }}')), max_cycle)
            ) as cycle
        from last_cycle
    ),

    bar_source as (
        select s.cycle, max(m.cycle) as bar_source_cycle
        from cycle_spine as s
        inner join measured_cycles as m on m.cycle <= s.cycle
        group by s.cycle
    )

select
    b.cycle,
    sm.days_out,
    b.bar_source_cycle,
    sm.reference_user_count,
    sm.actor_count,
    sm.raw_action_day_bar,
    sm.is_measured,
    sm.action_day_bar
from bar_source as b
inner join smoothed as sm on sm.cycle = b.bar_source_cycle
