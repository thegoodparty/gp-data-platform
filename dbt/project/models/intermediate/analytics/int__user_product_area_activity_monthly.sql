-- Active days per user, product, product area and month, for comparing where
-- one person spends their days with their peers. Same activity rule as
-- int__user_activity_monthly: a non-machine Win or Serve event.
--
-- product_active_days repeats on every area row of the month because a day can
-- touch several areas, so it cannot be summed from them. Days in different
-- months never overlap, so both columns sum exactly across months.
{{ config(materialized="table") }}

with
    keys as (select user_id from {{ ref("int__user_resolved_keys") }}),

    catalog as (
        select event_type, family, is_win, is_serve
        from {{ ref("int__amplitude_event_catalog") }}
        where (is_win or is_serve) and not is_machine_emitted
    ),

    event_days as (
        select distinct
            try_cast(e.user_id as bigint) as user_id,
            case when c.is_serve then 'serve' else 'win' end as product,
            -- The catalog files every Serve event under one family, so Serve
            -- areas come from the event name's prefix ("Polls - ...").
            case
                when c.is_serve then split_part(e.event_type, ' - ', 1) else c.family
            end as product_area,
            date(e.event_time) as activity_date
        from {{ ref("stg_airbyte_source__amplitude_api_events") }} as e
        join catalog as c on c.event_type = e.event_type
        where
            try_cast(e.user_id as bigint) is not null
            and e.event_time >= '{{ var("product_telemetry_start") }}'
    ),

    area_months as (
        select
            user_id,
            product,
            product_area,
            trunc(activity_date, 'MM') as activity_month,
            count(*) as area_active_days
        from event_days
        group by 1, 2, 3, 4
    ),

    product_months as (
        select
            user_id,
            product,
            trunc(activity_date, 'MM') as activity_month,
            count(distinct activity_date) as product_active_days
        from event_days
        group by 1, 2, 3
    )

select
    a.user_id,
    a.product,
    a.product_area,
    a.activity_month,
    a.area_active_days,
    p.product_active_days,
    a.activity_month = trunc(current_date(), 'MM') as is_partial_month
from area_months as a
join product_months as p using (user_id, product, activity_month)
join keys as k using (user_id)
