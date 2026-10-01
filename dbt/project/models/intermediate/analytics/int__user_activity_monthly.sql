-- One row per user per calendar month, from the user's first active month to
-- the current one. The single monthly activity definition: the profile row's
-- rolling flags and growth_state and the cohort history all read it, so the
-- chart and the row cannot disagree.
--
-- Months with no activity are kept as zero-day rows, because churned and
-- dormant are states of a month in which nothing happened.
{{ config(materialized="table") }}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    catalog as (
        select event_type, is_machine_emitted
        from {{ ref("int__amplitude_event_catalog") }}
    ),

    active_months as (
        select
            try_cast(e.user_id as bigint) as user_id,
            trunc(e.event_time, 'MM') as activity_month,
            count(distinct date(e.event_time)) as activity_days
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join catalog as c on c.event_type = e.event_type
        where
            try_cast(e.user_id as bigint) is not null
            and not c.is_machine_emitted
            and e.event_time >= '{{ var("product_telemetry_start") }}'
        group by 1, 2
    ),

    user_months as (select a.* from active_months as a join keys as k using (user_id)),

    first_months as (
        select user_id, min(activity_month) as first_month
        from user_months
        group by user_id
    ),

    calendar as (
        select
            explode(
                sequence(
                    date '{{ var("product_telemetry_start") }}',
                    trunc(current_date(), 'MM'),
                    interval 1 month
                )
            ) as activity_month
    ),

    grid as (
        select
            f.user_id,
            cal.activity_month,
            coalesce(um.activity_days, 0) as activity_days
        from first_months as f
        join calendar as cal on cal.activity_month >= f.first_month
        left join user_months as um using (user_id, activity_month)
    ),

    lagged as (
        select
            *,
            lag(activity_days, 1, 0) over (
                partition by user_id order by activity_month
            ) as days_prev_1,
            lag(activity_days, 2, 0) over (
                partition by user_id order by activity_month
            ) as days_prev_2,
            -- The grid starts at the user's first active month, so its first
            -- row is the month they were new.
            activity_month
            = min(activity_month) over (partition by user_id) as is_first_month
        from grid
    )

select
    user_id,
    activity_month,
    activity_days,
    -- 'dormant' is kept apart from 'churned' because a user idle for three
    -- months or more is no longer one we just lost.
    case
        when activity_days > 0 and is_first_month
        then 'new'
        when activity_days > 0 and days_prev_1 > 0
        then 'retained'
        when activity_days > 0
        then 'resurrected'
        when days_prev_1 > 0 or days_prev_2 > 0
        then 'churned'
        else 'dormant'
    end as growth_state,
    -- The current month is still filling. On the 1st it reads zero new and zero
    -- retained until that day's events load, so a trend excludes it by default.
    activity_month = trunc(current_date(), 'MM') as is_partial_month
from lagged
