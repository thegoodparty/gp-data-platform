{{ config(materialized="view") }}

/*
    Exposure of stg_airbyte_source__amplitude_api_events into mart_analytics
    (Sigma reads marts only). View, not table: a thin projection over a large
    staging table doesn't justify storage duplication.

    family / is_win / is_recurrent / is_dashboard_view come from the
    amplitude_event_taxonomy macros so classification has a single source of
    truth shared with int__amplitude_event_catalog; consumers must not re-derive
    product classification from event_type strings.

    is_dashboard_view is exposed separately because is_recurrent cannot express
    it: the dashboard-view union includes a page-path leg ('Viewed' on
    '/dashboard') and is_recurrent is an event-name allowlist. Filtering
    dashboard activity on is_recurrent alone undercounts, and reads near-zero
    after 2026-07-31 when the last named dashboard event died.
*/
select
    *,
    {{ amplitude_event_family("event_type") }} as family,
    family like 'win_%' as is_win,
    family = 'serve' as is_serve,
    -- null-safe: IN-list yields null (not false) on a null event_type
    coalesce({{ amplitude_event_is_recurrent("event_type") }}, false) as is_recurrent,
    -- null-safe for the same reason: the union's OR yields null on a null event_type
    coalesce(
        {{ is_dashboard_view_event("event_type", "event_properties:path::string") }},
        false
    ) as is_dashboard_view
from {{ ref("stg_airbyte_source__amplitude_api_events") }}
