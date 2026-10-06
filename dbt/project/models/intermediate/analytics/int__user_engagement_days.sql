-- One row per user per day on which they viewed their campaign dashboard or
-- reached voters through the product. The daily grain the power-user bar is
-- measured on: it counts distinct days, so a busy day counts once.
--
-- Views read the governed dashboard-view union, which survives the product's
-- renames of that event. A view by staff acting on the candidate's behalf is
-- not the candidate using the product, so it is dropped; the marker exists from
-- 2026-03-24, and earlier impersonated views cannot be told apart.
--
-- Sends read the same legs as is_activated, so "sent" here and activation
-- cannot drift apart.
{{ config(materialized="table") }}

with
    events as (
        select
            try_cast(user_id as bigint) as user_id,
            event_type,
            date(event_time) as activity_date,
            event_properties:path::string as page_path,
            event_properties:method::string as outreach_method,
            event_properties:product::string as outreach_product,
            coalesce(event_properties:impersonation::string, 'false')
            = 'true' as is_impersonated
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            try_cast(user_id as bigint) is not null
            and event_time >= '{{ var("product_telemetry_start") }}'
    ),

    flagged as (
        select
            user_id,
            activity_date,
            {{ is_dashboard_view_event("event_type", "page_path") }}
            and not is_impersonated as is_dashboard_view,
            {{
                is_outreach_activation_event(
                    "event_type", "outreach_method", "outreach_product"
                )
            }} as is_activation_event
        from events
    )

select
    user_id,
    activity_date,
    bool_or(is_dashboard_view) as has_dashboard_view,
    bool_or(is_activation_event) as has_activation_event
from flagged
where is_dashboard_view or is_activation_event
group by user_id, activity_date
