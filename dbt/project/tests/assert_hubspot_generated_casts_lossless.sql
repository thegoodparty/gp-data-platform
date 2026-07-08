-- Committed guard against silent cast loss on the generated hubspot contact
-- columns. The generation macro uses try_cast/cast_to_boolean on purpose
-- (the model and its mart are views, so a raising cast would error inside
-- Sigma at query time) — the trade-off is that a malformed future HubSpot
-- value becomes NULL silently. This test makes that loss loud: for every
-- typed generated column, non-empty raw values must equal cast survivors.
-- Warn severity: one bad manually-entered HubSpot value should page nobody,
-- but it must surface in the run output for follow-up.
-- depends_on: {{ ref("hubspot_contact_property_columns") }}
{{ config(severity="warn") }}

{%- set typed = [] %}
{%- for p in hubspot_generated_contact_properties() %}
    {%- if p.cast_type != "string" %} {% do typed.append(p) %} {% endif %}
{%- endfor %}

with
    raw_counts as (
        select
            {%- for p in typed %}
                count(
                    nullif(get_json_object(properties, '$.{{ p.internal_name }}'), '')
                ) as {{ p.column_name }}{{ "," if not loop.last }}
            {%- endfor %}
        from {{ source("airbyte_source", "hubspot_api_contacts") }}
    ),
    model_counts as (
        select
            {%- for p in typed %}
                count(
                    {{ p.column_name }}
                ) as {{ p.column_name }}{{ "," if not loop.last }}
            {%- endfor %}
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
    ),
    raw_long as (
        select
            stack(
                {{ typed | length }},
                {%- for p in typed %}
                    '{{ p.column_name }}', {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            ) as (column_name, raw_nonempty)
        from raw_counts
    ),
    model_long as (
        select
            stack(
                {{ typed | length }},
                {%- for p in typed %}
                    '{{ p.column_name }}', {{ p.column_name }}{{ "," if not loop.last }}
                {%- endfor %}
            ) as (column_name, model_nonnull)
        from model_counts
    )
select
    column_name,
    raw_nonempty,
    model_nonnull,
    raw_nonempty - model_nonnull as values_lost_to_cast
from raw_long
join model_long using (column_name)
where raw_nonempty != model_nonnull
