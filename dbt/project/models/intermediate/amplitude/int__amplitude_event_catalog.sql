{{ config(materialized="table", meta={"recency_window_days": 30}) }}

/*
    int__amplitude_event_catalog

    Purpose:
        Read-only warehouse catalog of Amplitude events: event behavior
        (family classification, volume, recency) combined with Amplitude Govern
        metadata, one row per event_type. Renamed from int__amplitude_event_taxonomy
        (DATA-2005): the model outgrew pure classification once Govern metadata and
        recency were added.

        It intentionally does NOT compute a staleness verdict (is_retired /
        is_unofficial / staleness_reason). Code-presence provenance lives as a CSV
        in this repo (analytics/data/amplitude_event_provenance.csv, DATA-2014). The
        relevance / up-to-dateness judgment is made at check time by an agent or the
        health monitor (DATA-1952) reconciling that CSV with this table, so a
        live-firing event whose code was removed is never silently marked retired.

    Grain:
        One row per distinct event_type seen in the raw event stream.

    Sources:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}            (behavior)
        {{ ref('stg_airbyte_source__amplitude_taxonomy_event_type') }}   (Govern metadata)

    Columns:
        Classification (macro-derived, authoritative):
        - event_type        : distinct Amplitude event name (primary key)
        - family            : product-feature bucket (see amplitude_event_family macro)
        - is_win            : Win-product candidate-attributed family (family like 'win_%')
        - is_recurrent      : recurrent activity vs one-off lifecycle milestone
        Volume / recency:
        - first_seen_date   : first appearance in the raw stream
        - last_seen_date    : most recent appearance in the raw stream
        - event_count       : total occurrences (all-time)
        - event_count_30d   : occurrences in the trailing 30 days
        Govern metadata (LEFT JOIN; namespaced govern_*, mostly uncurated today):
        - govern_category, govern_description, govern_display_name,
          govern_is_active, govern_is_hidden, govern_tags, govern_owner
        - in_govern_taxonomy: whether a Govern row matched at all (distinguishes
                              "not in Govern" from "in Govern but uncurated")

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
