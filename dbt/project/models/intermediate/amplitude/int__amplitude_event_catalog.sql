{{ config(materialized="table", meta={"recency_window_days": 30}) }}

/*
    int__amplitude_event_catalog

    Read-only warehouse catalog of Amplitude events: behavior (family
    classification, volume, recency) joined with Amplitude Govern metadata,
    one row per event_type.

    Notes:
        - Classification is pattern-based, so new event_types in known families
          classify automatically. family is authoritative; govern_category is
          descriptive metadata only and is not used in any derived logic.
        - Materialized as a table (not a view) because external notebooks SELECT/JOIN
          it directly via databricks-sql-connector and cannot call dbt macros.
*/
with
    event_stream as (
        select
            event_type,
            min(cast(event_time as date)) as first_seen_date,
            max(cast(event_time as date)) as last_seen_date,
            count(*) as event_count,
            count_if(
                cast(event_time as date) >= date_sub(current_date(), 30)
            ) as event_count_30d
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where event_type is not null
        group by event_type
    ),

    govern as (
        select
            event_type,
            category_name as govern_category,
            description as govern_description,
            display_name as govern_display_name,
            is_active as govern_is_active,
            coalesce(is_hidden_from_dropdowns, false)
            or coalesce(is_hidden_from_persona_results, false)
            or coalesce(is_hidden_from_pathfinder, false)
            or coalesce(is_hidden_from_timeline, false) as govern_is_hidden,
            tags as govern_tags,
            owner as govern_owner
        from {{ ref("stg_airbyte_source__amplitude_taxonomy_event_type") }}
    )

select
    s.event_type,
    {{ amplitude_event_family("s.event_type") }} as family,
    family like 'win_%' as is_win,
    {{ amplitude_event_is_recurrent("s.event_type") }} as is_recurrent,
    s.first_seen_date,
    s.last_seen_date,
    s.event_count,
    s.event_count_30d,
    g.govern_category,
    g.govern_description,
    g.govern_display_name,
    g.govern_is_active,
    g.govern_is_hidden,
    g.govern_tags,
    g.govern_owner,
    g.event_type is not null as in_govern_taxonomy
from event_stream as s
left join govern as g on s.event_type = g.event_type
