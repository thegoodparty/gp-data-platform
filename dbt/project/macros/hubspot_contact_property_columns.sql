{#-
    Accessors over the hubspot_contact_property_columns seed: the registry of
    HubSpot contact properties surfaced on stg_airbyte_source__hubspot_api_contacts.
    status='generated' rows are emitted by the model's loop; status='existing'
    is reserved for documenting columns the model exposes outside the loop.
    Models calling the accessor read the seed via run_query, so they must
    declare: -- depends_on: {{ ref("hubspot_contact_property_columns") }}

    The loop runs in int__hubspot_contacts_history, not in the staging model:
    the history table stores the typed columns, and staging projects them. Any
    new caller is an intermediate model, not a staging one.

    Adding a status='generated' row therefore adds a column to an incremental
    table, which takes three steps: add the seed row, full-refresh the contacts
    table in the Lakeflow pipeline (no schema evolution), then run dbt once with
    --vars '{hubspot_contacts_history_rescan: true}'. The model appends the column
    on that run, and the rescan drops the lastmodifieddate watermark so the
    already-unchanged rows are merged again and the column is filled; without it
    only contacts that changed since the last run would get a value.
-#}
{% macro hubspot_lakeflow_absent_properties() %}
    {#- Registry properties with no Lakeflow column, so the flat-column half of
        the expression is skipped for them. Declared as a constant rather than
        read from information_schema so a build never depends on a metadata
        query succeeding. -#}
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
    {#- Registry rows to generate, as dicts, in seed order. Empty at parse
        time (execute=false). flat_column is the Lakeflow column holding the
        same property, or none when Lakeflow does not carry it. -#}
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
    {#- Typed extraction of one HubSpot property. Two producers, one shape: the
        raw input is the Lakeflow flat column when this is a Lakeflow row, else
        the `properties` JSON path. HubSpot sends '' (not null) for unset values,
        so typed casts go through nullif first. try_cast everywhere typed: this
        model and its mart are views, so a plain cast would surface malformed
        future values as query-time errors in Sigma rather than a build failure -
        generated columns must never raise at query time. int stays int (not
        bigint) and decimal(38,10) mirrors the Airbyte-typed source columns:
        migrated columns keep their pre-registry types exactly. int_via_float
        keeps number_of_seats_available's float round-trip so decimal-formatted
        strings still land as ints instead of nulling out. boolean rows may carry
        a custom word-to-boolean value map from the seed.

        properties_column lets a caller pass the other side of the coalesce as a
        null literal, so a Lakeflow row has no `properties` and a frozen Airbyte
        row has no flat column. The flat side is cast to string before the
        coalesce on purpose: the `string` branch below applies no conversion, and
        one registry property (win_activated_user) arrives from Lakeflow as a real
        BOOLEAN, which would otherwise make that column a boolean where
        downstream expects a string. -#}
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
    {#- The change filter on int__hubspot_contacts_history's incremental branch.

        Default is >= not >: a row delivered with a timestamp equal to the stored
        maximum is re-merged rather than skipped, which costs one tie window of
        redundant work and closes the commonest skip. The residual, a row
        delivered strictly older than the stored maximum, waits for its next
        change; a per-row payload comparison would close it at the cost of
        scanning every row across every column on every run.

        hubspot_contacts_history_rescan drops the predicate, so one run re-merges
        every current Lakeflow row. That is always safe, because the current
        Lakeflow row is the newest truth for its id, and it is the recovery path
        after a new registry property (the watermark would otherwise skip every
        unchanged contact and leave the new column NULL) and after a Lakeflow
        full refresh leaves the watermark ahead of a re-loaded table. -#}
    {%- if not var("hubspot_contacts_history_rescan", false) %}
        where
            lastmodifieddate >= (
                select coalesce(max(lastmodifieddate), cast('1900-01-01' as timestamp))
                from {{ this }}
            )
    {%- endif %}
{% endmacro %}
