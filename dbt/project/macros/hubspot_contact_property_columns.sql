{#-
    Accessors over the hubspot_contact_property_columns seed: the registry of
    HubSpot contact properties surfaced on stg_airbyte_source__hubspot_api_contacts.
    status='generated' rows are emitted by the model's loop; status='existing'
    rows document hand-written columns and are never re-emitted.
    Models calling the accessor read the seed via run_query, so they must
    declare: -- depends_on: {{ ref("hubspot_contact_property_columns") }}
-#}
{% macro hubspot_generated_contact_properties(
    seed="hubspot_contact_property_columns"
) %}
    {#- Registry rows to generate, as dicts, in seed order. Empty at parse
        time (execute=false). -#}
    {%- set props = [] -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select internal_name, column_name, cast_type from "
            ~ ref(seed)
            ~ " where status = 'generated' order by sort_order"
        ) -%}
        {%- for row in results.rows -%}
            {%- do props.append(
                {
                    "internal_name": row[0],
                    "column_name": row[1],
                    "cast_type": row[2],
                }
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(props) }}
{% endmacro %}


{% macro hubspot_contact_property_expression(internal_name, cast_type) %}
    {#- Typed extraction of one HubSpot property from the raw `properties`
        JSON. HubSpot sends '' (not null) for unset values, so typed casts go
        through nullif first. try_cast everywhere typed: this model and its
        mart are views, so a plain cast would surface malformed future values
        as query-time errors in Sigma rather than a build failure — generated
        columns must never raise at query time. -#}
    {%- set raw = "get_json_object(properties, '$." ~ internal_name ~ "')" -%}
    {%- if cast_type == "boolean" -%} {{ cast_to_boolean(raw) }}
    {%- elif cast_type == "double" -%} try_cast({{ raw }} as double)
    {%- elif cast_type == "date" -%} try_cast(nullif({{ raw }}, '') as date)
    {%- elif cast_type == "timestamp" -%} try_cast(nullif({{ raw }}, '') as timestamp)
    {%- elif cast_type == "string" -%} {{ raw }}
    {%- else -%}
        {{
            exceptions.raise_compiler_error(
                "hubspot_contact_property_columns: unknown cast_type '"
                ~ cast_type
                ~ "' for property "
                ~ internal_name
            )
        }}
    {%- endif -%}
{% endmacro %}
