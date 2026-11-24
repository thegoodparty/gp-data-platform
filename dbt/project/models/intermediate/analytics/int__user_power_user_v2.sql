-- Power user definition v2, the definition for the 2026 cycle on: a candidate
-- who has reached voters through the product (the is_activated legs) and took
-- real actions in Win on at least as many days as the bar for their cycle and
-- distance from the election. Dashboard views and onboarding steps do not
-- count. One row per user with a candidacy.
--
-- The send must fall on or before the election: without it, a candidate who
-- explores every tool but never does the core action reads as a power user.
-- The bar is measured among senders too (int__power_user_bar_v2), so a power
-- user is in the top 15% of candidates who reached voters.
--
-- v1 (int__user_power_user: sent, plus dashboard view-days) stays as the
-- definition through mid-2026, so earlier reports reproduce. v1 rewarded a
-- candidate who checked the dashboard daily and never acted, and missed one
-- who ran outreach and research but rarely opened it.
--
-- Action days count from first use, in any year, up to the build date capped
-- at the election, so a campaign in progress is compared with completed
-- campaigns at the same point. The cycle is the election's calendar year.
-- Races more than 365 days out use the 365-day bar. Elections before telemetry
-- began have no bar and read as not a power user.
--
-- Moves with the build date, like every days-to-election column.
{{ config(materialized="table") }}

with
    candidates as (
        select
            user_id,
            candidacy_election_date as election_date,
            year(candidacy_election_date) as cycle,
            least(current_date(), candidacy_election_date) as measured_through,
            greatest(datediff(candidacy_election_date, current_date()), 0) as days_out
        from {{ ref("int__user_candidacy_profile") }}
        where has_candidacy
    ),

    counted_days as (
        select c.user_id, d.activity_date, d.action_features, d.has_send
        from candidates as c
        inner join
            {{ ref("int__user_action_days") }} as d
            on d.user_id = c.user_id
            and d.activity_date <= c.measured_through
    ),

    day_counts as (
        select user_id, count(*) as action_days, bool_or(has_send) as has_sent
        from counted_days
        group by user_id
    ),

    area_counts as (
        select user_id, count(distinct feature) as action_areas
        from counted_days
        lateral view explode(action_features) as feature
        group by user_id
    ),

    bar as (
        select cycle, days_out, action_day_bar from {{ ref("int__power_user_bar_v2") }}
    )

select
    c.user_id,
    c.cycle as power_user_cycle,
    c.days_out as power_user_days_out,
    coalesce(dc.action_days, 0) as power_user_action_days,
    coalesce(ac.action_areas, 0) as power_user_action_areas,
    b.action_day_bar as power_user_action_day_bar,
    coalesce(dc.has_sent, false) as power_user_has_sent,
    coalesce(dc.has_sent, false)
    and coalesce(
        coalesce(dc.action_days, 0) >= b.action_day_bar, false
    ) as is_power_user
from candidates as c
left join day_counts as dc using (user_id)
left join area_counts as ac using (user_id)
left join bar as b on b.cycle = c.cycle and b.days_out = least(c.days_out, 365)
