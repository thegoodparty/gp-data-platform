-- How many months a candidate engaged with Win before their own election day,
-- for reading win rate by usage. One row per user with an election date.
--
-- A month counts when the user was engaged that month (is_engaged), which is
-- built from the same events as the activation OKR. gp-api send rows are left
-- out on purpose: a p2p row reads completed once its scheduled day passes, and
-- a phone or door row reads in_progress once a list is built, so counting them
-- made "reached voters" disagree with activation.
{{ config(materialized="table") }}

with
    election as (
        select user_id, candidacy_election_date as election_date
        from {{ ref("int__user_candidacy_profile") }}
        where candidacy_election_date is not null
    ),

    engaged_months as (
        select a.user_id, a.activity_month
        from {{ ref("int__user_activity_monthly") }} as a
        join election as e using (user_id)
        where a.is_engaged and a.activity_month < e.election_date
    )

select
    e.user_id,
    coalesce(count(m.activity_month), 0) as win_engaged_months_before_election
from election as e
left join engaged_months as m using (user_id)
group by e.user_id
