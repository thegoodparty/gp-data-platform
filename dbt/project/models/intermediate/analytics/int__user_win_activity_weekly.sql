-- One row per user per week with Win activity. The weekly grain of the Win half
-- of int__user_activity_monthly, using the same rule: a non-machine Win event.
-- Not int__amplitude_win_activity_weekly, which counts only the recurrent
-- allowlist plus dashboard views and so reads fewer active weeks.
--
-- Only active weeks are kept. A week with no row was inactive, so a reader
-- joins its own week grid to this.
{{ config(materialized="table") }}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    catalog as (
        select event_type
        from {{ ref("int__amplitude_event_catalog") }}
        where is_win and not is_machine_emitted
    ),

    -- Weeks are UTC and Monday-anchored, as date_trunc('week') gives.
    active_weeks as (
        select
            try_cast(e.user_id as bigint) as user_id,
            cast(date_trunc('week', e.event_time) as date) as week_start_date,
            count(distinct date(e.event_time)) as win_activity_days
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join catalog as c on c.event_type = e.event_type
        where
            try_cast(e.user_id as bigint) is not null
            and e.event_time >= '{{ var("product_telemetry_start") }}'
        group by 1, 2
    )

select
    a.user_id,
    a.week_start_date,
    a.win_activity_days,
    -- The current week is still filling.
    a.week_start_date
    = cast(date_trunc('week', current_date()) as date) as is_partial_week
from active_weeks as a
join keys as k using (user_id)
