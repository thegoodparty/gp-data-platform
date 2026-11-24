-- Committed guard against silent cast loss on the generated hubspot contact
-- columns. The generation macro uses try_cast/cast_to_boolean on purpose
-- (the model and its mart are views, so a raising cast would error inside
-- Sigma at query time) — the trade-off is that a malformed future HubSpot
-- value becomes NULL silently. This test makes that loss loud: for every
-- typed generated column, non-empty raw values must equal cast survivors.
-- Error severity on purpose: HubSpot enforces property types at entry, so
-- real loss here means a format or schema drift (a property type changed,
-- a datetime representation shifted) — exactly the class of change that
-- must block a build rather than silently null a Sigma column.
-- Registry rows flagged cast_loss_expected are excluded: their source
-- properties legitimately carry non-conforming values (free-text ids,
-- out-of-map enum words) that the cast nulls by design, unchanged from the
-- pre-registry hand-written expressions.
--
-- The comparison is per producer, not per model. Staging now holds rows whose
-- raw input was a Lakeflow flat column and rows whose raw input was the Airbyte
-- `properties` JSON blob, so comparing one population's raw count against the
-- whole model's non-null count reads a legitimate Lakeflow-only value as loss and
-- hides a real Lakeflow cast loss behind an unrelated frozen value. The history
-- layer's _source column says which producer a row's winning values came from, so
-- each side is compared against its own raw input.
-- depends_on: {{ ref("hubspot_contact_property_columns") }}
-- depends_on: {{ ref("int__hubspot_contacts_history") }}
-- depends_on: {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
{{ config(severity="error") }}

{%- set typed = [] %}
{%- for p in hubspot_generated_contact_properties() %}
    {%- if p.cast_type != "string" and not p.cast_loss_expected %}
        {% do typed.append(p) %}
    {% endif %}
{%- endfor %}
{#- Lakeflow has no column for these, so its side of the comparison has no raw
    input to compare against. -#}
{%- set typed_lakeflow = [] %}
{%- for p in typed %}
    {%- if p.flat_column is not none %} {% do typed_lakeflow.append(p) %} {% endif %}
{%- endfor %}

{% if typed | length == 0 %}
    -- no typed generated rows in the registry: render a valid, always-empty
    -- statement instead of an illegal zero-arg stack()
    select
        cast(null as string) as column_name,
        cast(null as string) as source,
        cast(null as bigint) as raw_nonempty,
        cast(null as bigint) as model_nonnull,
        cast(null as bigint) as values_lost_to_cast
    where false
{% else %}
    with
        winning_source as (
            select history.id, history._source
            from {{ ref("int__hubspot_contacts_history") }} as history
            -- Only rows the model emits, so any filter it applies is honored here.
            where
                history.id is not null
                and history.id in (
                    select id
                    from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
                    where id is not null
                )
        ),
        airbyte_ids as (select id from winning_source where _source = 'airbyte'),
        lakeflow_ids as (select id from winning_source where _source = 'lakeflow'),
        raw_airbyte as (
            select
                {%- for p in typed %}
                    count(
                        nullif(
                            get_json_object(properties, '$.{{ p.internal_name }}'), ''
                        )
                    ) as {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            from {{ source("airbyte_source", "hubspot_api_contacts") }}
            where id in (select id from airbyte_ids where id is not null)
        ),
        raw_lakeflow as (
            select
                {%- for p in typed_lakeflow %}
                    count(
                        nullif(trim(cast({{ p.internal_name }} as string)), '')
                    ) as {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            from {{ source("lakeflow_source", "contacts") }}
            where hs_object_id in (select id from lakeflow_ids where id is not null)
        ),
        model_airbyte as (
            select
                {%- for p in typed %}
                    count(
                        {{ p.column_name }}
                    ) as {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
            where id in (select id from airbyte_ids where id is not null)
        ),
        model_lakeflow as (
            select
                {%- for p in typed_lakeflow %}
                    count(
                        {{ p.column_name }}
                    ) as {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
            where id in (select id from lakeflow_ids where id is not null)
        ),
        raw_airbyte_long as (
            select
                stack(
                    {{ typed | length }},
                    {%- for p in typed %}
                        '{{ p.column_name }}',
                        {{ p.column_name }}{{ "," if not loop.last }}
                    {%- endfor %}
                ) as (column_name, raw_nonempty)
            from raw_airbyte
        ),
        model_airbyte_long as (
            select
                stack(
                    {{ typed | length }},
                    {%- for p in typed %}
                        '{{ p.column_name }}',
                        {{ p.column_name }}{{ "," if not loop.last }}
                    {%- endfor %}
                ) as (column_name, model_nonnull)
            from model_airbyte
        ),
        raw_lakeflow_long as (
            select
                stack(
                    {{ typed_lakeflow | length }},
                    {%- for p in typed_lakeflow %}
                        '{{ p.column_name }}',
                        {{ p.column_name }}{{ "," if not loop.last }}
                    {%- endfor %}
                ) as (column_name, raw_nonempty)
            from raw_lakeflow
        ),
        model_lakeflow_long as (
            select
                stack(
                    {{ typed_lakeflow | length }},
                    {%- for p in typed_lakeflow %}
                        '{{ p.column_name }}',
                        {{ p.column_name }}{{ "," if not loop.last }}
                    {%- endfor %}
                ) as (column_name, model_nonnull)
            from model_lakeflow
        ),
        compared as (
            select 'airbyte' as source, r.column_name, r.raw_nonempty, m.model_nonnull
            from raw_airbyte_long as r
            join model_airbyte_long as m using (column_name)
            union all
            select 'lakeflow' as source, r.column_name, r.raw_nonempty, m.model_nonnull
            from raw_lakeflow_long as r
            join model_lakeflow_long as m using (column_name)
        )

    select
        column_name,
        source,
        raw_nonempty,
        model_nonnull,
        raw_nonempty - model_nonnull as values_lost_to_cast
    from compared
    where raw_nonempty != model_nonnull
{% endif %}
