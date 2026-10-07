-- Whether each candidate is a power user: reached voters through the product at
-- least once, and viewed their dashboard on at least as many days as the bar
-- for their distance from the election. One row per user with a candidacy.
--
-- Both counts run up to the build date, capped at the election, so a campaign
-- in progress is compared with completed campaigns at the same point. A race
-- already decided is measured over the whole campaign against the
-- election-day bar. Races more than 365 days out use the 365-day bar.
--
-- Moves with the build date, like every days-to-election column.
{{ config(materialized="table") }}

with
    candidates as (
        select
            user_id,
            candidacy_election_date as election_date,
            least(current_date(), candidacy_election_date) as measured_through,
            greatest(datediff(candidacy_election_date, current_date()), 0) as days_out
        from {{ ref("int__user_candidacy_profile") }}
        where has_candidacy
    ),

    activity as (
        select
            c.user_id,
            count_if(d.has_dashboard_view) as view_days,
            coalesce(bool_or(d.has_activation_event), false) as has_sent
        from candidates as c
        left join
            {{ ref("int__user_engagement_days") }} as d
            on d.user_id = c.user_id
            and d.activity_date <= c.measured_through
        group by c.user_id
    ),

    bar as (select days_out, view_day_bar from {{ ref("int__power_user_bar") }})

select
    c.user_id,
    c.days_out as power_user_days_out,
    a.view_days as power_user_view_days,
    b.view_day_bar as power_user_view_day_bar,
    a.has_sent as power_user_has_sent,
    a.has_sent and a.view_days >= b.view_day_bar as is_power_user
from candidates as c
inner join activity as a using (user_id)
left join bar as b on b.days_out = least(c.days_out, 365)
