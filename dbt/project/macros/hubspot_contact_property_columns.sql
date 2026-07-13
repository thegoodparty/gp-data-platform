{#-
    Accessors over the hubspot_contact_property_columns seed: the registry of
    HubSpot contact properties surfaced on stg_airbyte_source__hubspot_api_contacts.
    status='generated' rows are emitted by the model's loop; status='existing'
    is reserved for documenting columns the model exposes outside the loop.
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
            "select internal_name, column_name, cast_type, "
            ~ "boolean_true_value, boolean_false_value from "
            ~ ref(seed)
            ~ " where status = 'generated' order by sort_order"
        ) -%}
        {%- for row in results.rows -%}
            {%- do props.append(
                {
                    "internal_name": row[0],
                    "column_name": row[1],
                    "cast_type": row[2],
                    "true_value": row[3],
                    "false_value": row[4],
                }
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(props) }}
{% endmacro %}


{% macro hubspot_contact_property_expression(
    internal_name, cast_type, true_value=none, false_value=none
) %}
    {#- Typed extraction of one HubSpot property from the raw `properties`
        JSON. HubSpot sends '' (not null) for unset values, so typed casts go
        through nullif first. try_cast everywhere typed: this model and its
        mart are views, so a plain cast would surface malformed future values
        as query-time errors in Sigma rather than a build failure — generated
        columns must never raise at query time. int stays int (not bigint)
        and decimal(38,10) mirrors the Airbyte-typed source columns: migrated
        columns keep their pre-registry types exactly. int_via_float keeps
        number_of_seats_available's float round-trip so decimal-formatted
        strings still land as ints instead of nulling out. boolean rows may
        carry a custom word-to-boolean value map from the seed. -#}
    {%- set raw = "get_json_object(properties, '$." ~ internal_name ~ "')" -%}
    {%- if cast_type == "boolean" -%}
        {%- if true_value -%} {{ cast_to_boolean(raw, [true_value], [false_value]) }}
        {%- else -%} {{ cast_to_boolean(raw) }}
        {%- endif -%}
    {%- elif cast_type == "double" -%} try_cast(nullif({{ raw }}, '') as double)
    {%- elif cast_type == "bigint" -%} try_cast(nullif({{ raw }}, '') as bigint)
    {%- elif cast_type == "int" -%} try_cast(nullif({{ raw }}, '') as int)
    {%- elif cast_type == "int_via_float" -%}
        try_cast(try_cast(nullif({{ raw }}, '') as float) as int)
    {%- elif cast_type == "decimal(38,10)" -%}
        try_cast(nullif({{ raw }}, '') as decimal(38, 10))
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
