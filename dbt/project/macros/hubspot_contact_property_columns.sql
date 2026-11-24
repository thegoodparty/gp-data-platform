{#-
    Accessors over the hubspot_contact_property_columns seed registry.
    status='generated' rows are emitted by the loop; status='existing' documents
    columns the model exposes outside it. Callers read the seed with run_query, so
    they must declare: -- depends_on: {{ ref("hubspot_contact_property_columns") }}

    The loop runs in int__hubspot_contacts_history, not in staging, so a new
    status='generated' row adds a column to an incremental table. Three steps: add
    the seed row, full-refresh the contacts table in the Lakeflow pipeline, then
    run dbt once with --vars '{hubspot_contacts_history_rescan: true}' to fill it.
-#}
{% macro hubspot_lakeflow_absent_properties() %}
    {#- Registry properties with no Lakeflow column. A constant rather than an
        information_schema read, so a build never depends on metadata. -#}
    {{
        return(
            [
                "hs_feedback_last_csat_survey_date",
                "hs_feedback_last_csat_survey_follow_up",
                "hs_feedback_last_csat_survey_rating",
                "hs_feedback_last_nps_rating_number",
                "hs_is_unworked",
                "hs_v2_date_entered_current_stage",
                "hubspotscore",
                "serve__days_between_poll_scheduled_and_launched",
                "serve__duration_between_results_received_and_launch_date",
            ]
        )
    }}
{% endmacro %}


{% macro hubspot_generated_contact_properties(
    seed="hubspot_contact_property_columns"
) %}
    {#- Registry rows to generate, in seed order. flat_column is the Lakeflow
        column holding the same property, or none when Lakeflow lacks it. -#}
    {%- set absent = hubspot_lakeflow_absent_properties() -%}
    {%- set props = [] -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select internal_name, column_name, cast_type, "
            ~ "boolean_true_value, boolean_false_value, cast_loss_expected from "
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
                    "cast_loss_expected": row[5],
                    "flat_column": (none if row[0] in absent else row[0]),
                }
            ) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(props) }}
{% endmacro %}


{% macro hubspot_contact_property_expression(
    internal_name,
    cast_type,
    true_value=none,
    false_value=none,
    flat_column=none,
    properties_column="properties"
) %}
    {#- Typed extraction of one HubSpot property, from the Lakeflow flat column
        when present else the `properties` JSON path. HubSpot sends '' not null, so
        typed casts go through nullif. try_cast throughout: the model and its mart
        are views, so a raising cast would error in Sigma at query time instead of
        failing a build. Types mirror the pre-registry columns exactly.

        The flat side is cast to string before the coalesce because the `string`
        branch applies no conversion, and win_activated_user arrives as a real
        BOOLEAN which would otherwise change the column's type. -#}
    {%- set raw = (
        "get_json_object("
        ~ properties_column
        ~ ", '$."
        ~ internal_name
        ~ "')"
    ) -%}
    {%- if flat_column is not none -%}
        {%- set raw = "coalesce(cast(" ~ flat_column ~ " as string), " ~ raw ~ ")" -%}
    {%- endif -%}
    {%- if cast_type == "boolean" -%}
        {%- if true_value -%} {{ cast_to_boolean(raw, [true_value], [false_value]) }}
        {%- else -%} {{ cast_to_boolean(raw) }}
        {%- endif -%}
    {%- elif cast_type == "double" -%} try_cast(nullif({{ raw }}, '') as double)
    {%- elif cast_type == "bigint" -%} try_cast(nullif({{ raw }}, '') as bigint)
    {%- elif cast_type == "int" -%}
        {#- int first, then a double round-trip: Lakeflow hands some int
            properties over float-formatted ("349000000.0"), which a plain
            try_cast to int nulls. Double rather than float because float is
            32-bit here and rounds large values. Measured: the fallback recovers
            every float-formatted value and changes no value that already
            parsed, on either producer. -#}
        coalesce(
            try_cast(nullif({{ raw }}, '') as int),
            try_cast(try_cast(nullif({{ raw }}, '') as double) as int)
        )
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


{% macro hubspot_contacts_history_watermark() %}
    {#- The incremental change filter. >= not >, so a row tied with the stored
        maximum is re-merged rather than skipped; the residual is a row delivered
        strictly older, which waits for its next change. The rescan var drops the
        predicate so one run re-merges every current Lakeflow row, which is safe
        because that row is the newest truth, and is how a new column gets filled
        after a registry change. -#}
    {%- if not var("hubspot_contacts_history_rescan", false) %}
        where
            lastmodifieddate >= (
                select coalesce(max(lastmodifieddate), cast('1900-01-01' as timestamp))
                from {{ this }}
            )
    {%- endif %}
{% endmacro %}
