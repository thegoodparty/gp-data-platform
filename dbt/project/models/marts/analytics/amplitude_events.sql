{{ config(materialized="view") }}

/*
    Exposure of stg_airbyte_source__amplitude_api_events into mart_analytics
    (Sigma reads marts only). View, not table: a thin projection over a large
    staging table doesn't justify storage duplication.

    family / is_win / is_recurrent come from the amplitude_event_taxonomy
    macros so classification has a single source of truth shared with
    int__amplitude_event_catalog; consumers must not re-derive product
    classification from event_type strings.
*/
select
    *,
    {{ amplitude_event_family("event_type") }} as family,
    family like 'win_%' as is_win,
    -- null-safe: IN-list yields null (not false) on a null event_type
    coalesce({{ amplitude_event_is_recurrent("event_type") }}, false) as is_recurrent
from {{ ref("stg_airbyte_source__amplitude_api_events") }}
