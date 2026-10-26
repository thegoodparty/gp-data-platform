{{ config(materialized="view") }}

/*
    Exposure of int__amplitude_events_classified into mart_analytics (Sigma
    reads marts only). View, not table: a thin projection over a large event
    table doesn't justify storage duplication.

    family and product_area come from int__amplitude_event_catalog, and is_win /
    is_serve from the event's resolved product, so a shared feature counts for
    the product it was used from. is_recurrent and is_dashboard_view come from
    the amplitude_event_taxonomy macros. Consumers must not re-derive product
    classification from event_type strings.

    is_dashboard_view is exposed separately because is_recurrent cannot express
    it: the dashboard-view union includes a page-path leg ('Viewed' on
    '/dashboard') and is_recurrent is an event-name allowlist. Filtering
    dashboard activity on is_recurrent alone undercounts, and reads near-zero
    after 2026-07-31 when the last named dashboard event died.
*/
select
    * except (family, is_machine_emitted, is_recurrent, event_product),
    -- The catalog skips a null event_type, which the family macro calls 'other'.
    coalesce(family, 'other') as family,
    coalesce(event_product = 'win', false) as is_win,
    coalesce(event_product = 'serve', false) as is_serve,
    -- null-safe: IN-list yields null (not false) on a null event_type
    coalesce({{ amplitude_event_is_recurrent("event_type") }}, false) as is_recurrent,
    -- null-safe for the same reason: the union's OR yields null on a null event_type
    coalesce(
        {{ is_dashboard_view_event("event_type", "event_properties:path::string") }},
        false
    ) as is_dashboard_view
from {{ ref("int__amplitude_events_classified") }}
