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

    -- event_product, not the catalog's is_win/is_serve, so shared features count
    -- toward the product they were used from.
    active_months as (
        select
            try_cast(e.user_id as bigint) as user_id,
            trunc(e.event_time, 'MM') as activity_month,
            count(distinct date(e.event_time)) as activity_days,
            count(
                distinct case when e.event_product = 'serve' then date(e.event_time) end
            ) as serve_activity_days,
            count(
                distinct case when e.event_product = 'win' then date(e.event_time) end
            ) as win_activity_days
        from {{ ref("int__amplitude_events_classified") }} as e
        where
            try_cast(e.user_id as bigint) is not null
            and not e.is_machine_emitted
            and e.event_time >= '{{ var("product_telemetry_start") }}'
        group by 1, 2
    ),

    -- The governed activation legs, read straight from events: the OKR does not
    -- drop machine-emitted events, so neither does engagement.
    engaged_months as (
        select
            try_cast(e.user_id as bigint) as user_id,
            trunc(e.event_time, 'MM') as activity_month,
            bool_or(
                {{
                    is_outreach_activation_event(
                        "e.event_type",
                        "e.event_properties:method::string",
                        "e.event_properties:product::string",
                    )
                }}
            ) as is_engaged,
            bool_or(
                {{
                    is_serve_activation_event(
                        "e.event_type",
                        "e.event_properties:method::string",
                        "e.event_properties:product::string",
                    )
                }}
            ) as is_engaged_serve
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        where
            try_cast(e.user_id as bigint) is not null
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
            coalesce(um.activity_days, 0) as activity_days,
            coalesce(um.serve_activity_days, 0) as serve_activity_days,
            coalesce(um.win_activity_days, 0) as win_activity_days,
            coalesce(em.is_engaged, false) as is_engaged,
            coalesce(em.is_engaged_serve, false) as is_engaged_serve
        from first_months as f
        join calendar as cal on cal.activity_month >= f.first_month
        left join user_months as um using (user_id, activity_month)
        left join engaged_months as em using (user_id, activity_month)
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
            lag(serve_activity_days, 1, 0) over (
                partition by user_id order by activity_month
            ) as serve_days_prev_1,
            lag(serve_activity_days, 2, 0) over (
                partition by user_id order by activity_month
            ) as serve_days_prev_2,
            min(case when serve_activity_days > 0 then activity_month end) over (
                partition by user_id
            ) as first_serve_month,
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
    {{ growth_state("activity_days", "days_prev_1", "days_prev_2", "is_first_month") }}
    as growth_state,
    -- The current month is still filling. On the 1st it reads zero new and zero
    -- retained until that day's events load, so a trend excludes it by default.
    activity_month = trunc(current_date(), 'MM') as is_partial_month,
    serve_activity_days,
    -- Null until the first Serve month: before that they were not a Serve user.
    case
        when activity_month >= first_serve_month
        then
            {{
                growth_state(
                    "serve_activity_days",
                    "serve_days_prev_1",
                    "serve_days_prev_2",
                    "activity_month = first_serve_month",
                )
            }}
    end as serve_growth_state,
    is_engaged,
    is_engaged_serve,
    -- activity_days counts every product; this is the Win share, for a Win
    -- monthly-active count that excludes days spent only in Serve.
    win_activity_days
from lagged
