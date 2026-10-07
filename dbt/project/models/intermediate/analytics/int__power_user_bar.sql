-- The power-user viewing bar at each distance from the election, 0 to 365 days
-- out. One row per days_out.
--
-- Measured on completed races, so every reference user had their whole
-- campaign to accumulate view-days. At each days_out the bar is the rule the
-- power-user report used: the 85th percentile of view-days among users who had
-- viewed at all by that point, rounded up. Measuring at the same distance from
-- the election is what lets a user mid-campaign be compared fairly; a single
-- end-of-campaign bar would read almost no one as a power user months out.
--
-- Reference races start six months after product telemetry, so a campaign
-- that began before tracking does not read as idle.
{{ config(materialized="table") }}

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

    reference_users as (
        select c.user_id, c.candidacy_election_date as election_date
        from {{ ref("int__user_candidacy_profile") }} as c
        left anti join internal_users as i on i.user_id = c.user_id
        left anti join demo_only_users as d on d.user_id = c.user_id
        where
            c.has_candidacy
            and c.candidacy_election_date
            >= add_months(date('{{ var("product_telemetry_start") }}'), 6)
            and c.candidacy_election_date < current_date()
    ),

    view_days as (
        select r.user_id, r.election_date, d.activity_date
        from reference_users as r
        inner join
            {{ ref("int__user_engagement_days") }} as d
            on d.user_id = r.user_id
            and d.has_dashboard_view
            and d.activity_date <= r.election_date
    ),

    days_out_spine as (select explode(sequence(0, 365)) as days_out),

    view_days_at as (
        select s.days_out, r.user_id, count(v.activity_date) as view_days
        from days_out_spine as s
        cross join reference_users as r
        left join
            view_days as v
            on v.user_id = r.user_id
            and v.activity_date <= date_sub(r.election_date, s.days_out)
        group by s.days_out, r.user_id
    ),

    raw_bar as (
        select
            days_out,
            count(*) as reference_user_count,
            count_if(view_days > 0) as viewer_count,
            cast(
                ceil(percentile(view_days, 0.85) filter (where view_days > 0)) as int
            ) as raw_view_day_bar
        from view_days_at
        group by days_out
    )

-- Two corrections to the raw percentile, which is noisy far from the election.
-- A bar further out can never be stricter than one closer in, so each day takes
-- the lowest bar at or nearer the election. And below 500 viewers the rounded
-- percentile flips between neighbouring integers day to day, so those days
-- carry the last bar measured on enough viewers instead of their own. Viewer
-- counts only fall with distance, so every unreliable day lies beyond the
-- reliable ones.
select
    days_out,
    reference_user_count,
    viewer_count,
    raw_view_day_bar,
    viewer_count >= 500 as is_measured,
    min(case when viewer_count >= 500 then raw_view_day_bar end) over (
        order by days_out rows between unbounded preceding and current row
    ) as view_day_bar
from raw_bar
