{{ config(materialized="table", meta={"recency_window_days": 30}) }}

/*
    int__amplitude_event_catalog

    Read-only warehouse catalog of Amplitude events: behavior (family
    classification, volume, recency) joined with Amplitude Govern metadata,
    one row per event_type.

    Notes:
        - Classification is pattern-based, so new event_types in known families
          classify automatically. family is authoritative. The Govern `product:`
          and `surface:` tags fill gaps only: they place an event the name rules
          leave in 'other' and never move one they already place.
          govern_category is descriptive metadata only and is not used in any
          derived logic.
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
    ),

    classified as (
        select
            s.event_type,
            {{ amplitude_event_family("s.event_type") }} as name_rule_family,
            {{ amplitude_govern_tag_value("g.govern_tags", "product") }}
            as raw_govern_product,
            {{ amplitude_govern_tag_value("g.govern_tags", "surface") }}
            as govern_surface,
            -- Two product tags is a labelling error that govern_product nulls out.
            size(filter(g.govern_tags, t -> lower(trim(t)) like 'product:%'))
            > 1 as has_many_product_tags,
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
    ),

    declared as (
        select
            *,
            -- Only the three declared products count; any other label is a typo to
            -- ignore.
            case
                when raw_govern_product in ('win', 'serve', 'shared')
                then raw_govern_product
            end as govern_product,
            case
                when name_rule_family != 'other'
                then name_rule_family
                when govern_product = 'win'
                then 'win_declared'
                when govern_product = 'serve'
                then 'serve'
                when govern_product = 'shared'
                then 'shared'
                else 'other'
            end as family
        from classified
    )

select
    event_type,
    name_rule_family,
    govern_product,
    govern_surface,
    family,
    case
        when family like 'win_%' and name_rule_family = family
        then family
        when family = 'serve' or family in ('win_declared', 'shared')
        then
            coalesce(
                case when name_rule_family = 'other' then govern_surface end,
                split_part(event_type, ' - ', 1)
            )
    end as product_area,
    family like 'win_%' as is_win,
    family = 'serve' as is_serve,
    family = 'shared' as is_shared,
    coalesce(
        has_many_product_tags
        or (
            govern_product in ('win', 'serve')
            and (name_rule_family like 'win_%' or name_rule_family = 'serve')
            and (govern_product = 'win') != (name_rule_family like 'win_%')
        ),
        false
    ) as product_label_conflict,
    {{ amplitude_event_is_recurrent("event_type") }} as is_recurrent,
    {{ amplitude_event_is_machine_emitted("event_type") }} as is_machine_emitted,
    first_seen_date,
    last_seen_date,
    event_count,
    event_count_30d,
    govern_category,
    govern_description,
    govern_display_name,
    govern_is_active,
    govern_is_hidden,
    govern_tags,
    govern_owner,
    in_govern_taxonomy
from declared
