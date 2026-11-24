-- depends_on: {{ ref("hubspot_contact_property_columns") }}
{#-
    Never-delete contacts layer. Lakeflow mirrors HubSpot's current state, so a
    contact that merges away or is deleted simply stops being returned; Airbyte's
    frozen history stops at 2026-11-17. This table is the only copy of anything
    HubSpot has dropped since, which is what two things protect:

    - full_refresh=false: a --full-refresh reaching this model would rebuild it
      from current state and lose every post-cutover deletion permanently.
    - The seed branch unions the frozen Airbyte rows, Lakeflow winning. Every
      later run reads Lakeflow only. If an incremental run read the union, a
      contact Lakeflow updated and HubSpot then dropped would be absent from
      Lakeflow and present in the frozen table, so the merge on id would write
      that stale 2026-11-17 row over the newer one and the record would silently
      roll back.

    The registry loop runs here, not in the staging model: this table stores the
    typed columns the loop produces and staging projects them.
-#}
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        full_refresh=false,
    )
}}

{% set props = hubspot_generated_contact_properties() %}

{#- One typed column list per producer, same column names in the same order so a
    union lines up positionally. A Lakeflow row has no `properties` blob and a
    frozen Airbyte row has no flat column, so each side passes the other as a null
    literal and the expression's coalesce is exact rather than a precedence rule.
    The absent-nine properties carry flat_column=none, so they read the JSON path
    only: NULL on a Lakeflow row, and the real value on a frozen Airbyte row, which
    costs nothing because that is the one-time seed branch. -#}
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
    {#- Lakeflow only. A row missing from Lakeflow may have been deleted, and the
        frozen row for it is older than what this table already holds. -#}
    select
        cast(hs_object_id as string) as id,
        case
            when associatedcompanyid is not null and trim(associatedcompanyid) <> ''
            then to_json(array(cast(trim(associatedcompanyid) as string)))
        end as companies,
        cast(createdate as timestamp) as created_at,
        cast(lastmodifieddate as timestamp) as updated_at,
        -- Carried because the filter below reads it.
        cast(lastmodifieddate as timestamp) as lastmodifieddate
        {{ lakeflow_typed_columns }},
        -- Which producer the row's winning values came from. Not published: the
        -- staging model drops it. It makes the seed window auditable and it is
        -- what lets a cast-loss check compare within one source at a time.
        -- Last in the list in both branches on purpose: a Databricks merge maps
        -- the source columns to the target by position.
        'lakeflow' as _source
    from {{ source("lakeflow_source", "contacts") }}
    -- >= not >: a row delivered with a timestamp equal to the stored maximum is
    -- re-merged rather than skipped, which costs one tie window of redundant work
    -- and closes the commonest skip. The residual, a row delivered strictly older
    -- than the stored maximum, waits for its next change; a per-row payload
    -- comparison would close it at the cost of scanning every row across every
    -- column on every run.
    where
        lastmodifieddate >= (
            select coalesce(max(lastmodifieddate), cast('1900-01-01' as timestamp))
            from {{ this }}
        )

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

        -- Lakeflow wins for any id it has; the frozen rows fill in the contacts
        -- HubSpot has dropped since 2026-11-17, which is why the union lives in
        -- the seed branch alone.
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
