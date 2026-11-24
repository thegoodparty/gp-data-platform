-- depends_on: {{ ref("hubspot_contact_property_columns") }}
-- Never-delete contacts layer. Lakeflow mirrors HubSpot's current state, so a
-- merged-away or deleted contact stops being returned, and this table is the only
-- copy of anything dropped after the frozen Airbyte extract ends.
--
-- Two rules protect it. full_refresh=false, because a full refresh would rebuild
-- from current state and lose those rows permanently. And only the seed branch
-- unions frozen Airbyte (Lakeflow winning); later runs read Lakeflow alone, or a
-- stale frozen row would overwrite a newer one on the run after HubSpot drops it.
--
-- The registry loop runs here rather than in staging, so the columns are typed.
{#- on_schema_change so a later registry row becomes a column here instead of
    leaving staging to select a missing one. Appending alone leaves it NULL, since
    the watermark skips unchanged contacts, so one run with
    hubspot_contacts_history_rescan fills it. -#}
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        full_refresh=false,
        on_schema_change="append_new_columns",
    )
}}

{% set props = hubspot_generated_contact_properties() %}

{#- One typed column list per producer, same names in the same order. Each side
    passes the other as a null literal, so the expression's coalesce is exact
    rather than a precedence rule. The nine properties Lakeflow lacks carry
    flat_column=none and so read only the JSON path. -#}
{% set lakeflow_typed_columns %}
    {%- for prop in props %}
        , {{ hubspot_contact_property_expression(
            prop.internal_name,
            prop.cast_type,
            prop.true_value,
            prop.false_value,
            flat_column=prop.flat_column,
            properties_column="cast(null as string)",
        ) }} as {{ prop.column_name }}
    {%- endfor %}
{% endset %}

{% set airbyte_typed_columns %}
    {%- for prop in props %}
        , {{ hubspot_contact_property_expression(
            prop.internal_name,
            prop.cast_type,
            prop.true_value,
            prop.false_value,
            flat_column=none,
            properties_column="properties",
        ) }} as {{ prop.column_name }}
    {%- endfor %}
{% endset %}

{% if is_incremental() %}
    select
        cast(hs_object_id as string) as id,
        case
            when associatedcompanyid is not null and trim(associatedcompanyid) <> ''
            then to_json(array(cast(trim(associatedcompanyid) as string)))
        end as companies,
        cast(createdate as timestamp) as created_at,
        cast(lastmodifieddate as timestamp) as updated_at,
        -- Carried because the watermark filter reads it.
        cast(lastmodifieddate as timestamp) as lastmodifieddate
        {{ lakeflow_typed_columns }},
        -- Which producer the row won from. Not published; the cast-loss test
        -- compares within one source at a time with it.
        'lakeflow' as _source
    from
        {{ source("lakeflow_source", "contacts") }}
        {{ hubspot_contacts_history_watermark() }}

{% else %}
    with
        lakeflow_contacts as (
            select
                cast(hs_object_id as string) as id,
                case
                    when
                        associatedcompanyid is not null
                        and trim(associatedcompanyid) <> ''
                    then to_json(array(cast(trim(associatedcompanyid) as string)))
                end as companies,
                cast(createdate as timestamp) as created_at,
                cast(lastmodifieddate as timestamp) as updated_at,
                cast(lastmodifieddate as timestamp) as lastmodifieddate
                {{ lakeflow_typed_columns }}
            from {{ source("lakeflow_source", "contacts") }}
        ),

        airbyte_contacts as (
            select
                cast(id as string) as id,
                companies,
                cast(createdat as timestamp) as created_at,
                cast(updatedat as timestamp) as updated_at,
                cast(updatedat as timestamp) as lastmodifieddate
                {{ airbyte_typed_columns }}
            from {{ source("airbyte_source", "hubspot_api_contacts") }}
            where id not in (select id from lakeflow_contacts where id is not null)
        ),

        -- Lakeflow wins; frozen rows fill in the contacts HubSpot has dropped.
        seeded as (
            select lakeflow_contacts.*, 'lakeflow' as _source
            from lakeflow_contacts
            union all
            select airbyte_contacts.*, 'airbyte' as _source
            from airbyte_contacts
        )

    select *
    from seeded
{% endif %}
