-- How many months a candidate engaged with Win before their own election day,
-- for reading win rate by usage. One row per user with an election date.
--
-- A month counts when the user was engaged that month (is_engaged) or sent
-- committed Win outreach that month. The months are the activity table's own,
-- which start at the user's first tracked product month, so a send month before
-- that is not counted. This matches the win-rate-by-usage read it replaces;
-- widening it to every send month would change those numbers.
{{ config(materialized="table") }}

with
    election as (
        select user_id, candidacy_election_date as election_date
        from {{ ref("int__user_candidacy_profile") }}
        where candidacy_election_date is not null
    ),

    send_months as (
        select distinct user_id, cast(trunc(send_at, 'MM') as date) as activity_month
        from {{ ref("int__user_outreach_sends") }}
        where basis = 'committed' and product = 'win' and send_at <= current_timestamp()
    ),

    engaged_months as (
        select a.user_id, a.activity_month
        from {{ ref("int__user_activity_monthly") }} as a
        join election as e using (user_id)
        left join send_months as s using (user_id, activity_month)
        where
            (a.is_engaged or s.user_id is not null)
            and a.activity_month < e.election_date
    )

select
    e.user_id,
    coalesce(count(m.activity_month), 0) as win_engaged_months_before_election
from election as e
left join engaged_months as m using (user_id)
group by e.user_id
