{{ config(materialized="view") }}

/*
    int__amplitude_serve_activity

    Purpose:
        User-month intermediate model for canonical Serve MAU activity.
        Produces one row per user_id x activity_month from Amplitude events.

    Grain:
        One row per user_id (BIGINT) x activity_month.

    Source:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}

    Event -> column mapping:
        - Viewed + event_properties:path = '/dashboard/polls'
          + user_properties:`Serve Activated` = true
            -> activity presence for MAU
        - user_properties:email
            -> latest monthly email value for filtering/context
        - event_time
            -> first_activity_at, last_activity_at
        - event count
            -> poll_view_count
        - distinct DATE(event_time)
            -> activity_days

    Definition notes:
        - Uses AD-04 canonical Serve MAU filter.
        - Casts Amplitude string user_id with TRY_CAST(user_id AS BIGINT).
        - Excludes records where BIGINT cast fails.
        - Excludes internal @goodparty.org emails.
*/
with
    serve_activity as (
        select
            try_cast(user_id as bigint) as user_id,
            date_trunc('month', event_time) as activity_month,
            max_by(user_properties:email::string, event_time) as email,
            min(event_time) as first_activity_at,
            max(event_time) as last_activity_at,
            count(*) as poll_view_count,
            count(distinct date(event_time)) as activity_days
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            event_type = 'Viewed'
            and event_properties:path::string = '/dashboard/polls'
            and user_properties:`Serve Activated`::boolean = true
            and user_id is not null
            and try_cast(user_id as bigint) is not null
            and (
                user_properties:email::string not like '%@goodparty.org'
                or user_properties:email::string is null
            )
        group by 1, 2
    )

select *
from serve_activity
