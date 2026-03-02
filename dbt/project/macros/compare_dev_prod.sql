{% macro compare_dev_prod(
    model,
    dev_schema="dbt_dball",
    prod_schema="dbt",
    primary_key="techspeed_candidate_code",
    sample_size=10
) %}
    {#
        Compare a model between dev and prod schemas.

        Usage:
            dbt run-operation compare_dev_prod --args '{"model": "int__techspeed_candidates_clean"}'

            # With custom schemas or key:
            dbt run-operation compare_dev_prod --args '{"model": "int__techspeed_candidates_clean", "primary_key": "techspeed_candidate_code"}'

        Output:
            - Row counts for both schemas
            - Rows only in dev, only in prod
            - Per-column mismatch counts for rows that exist in both
            - Sample rows with differences
    #}
    {% set catalog = target.database %}
    {% set dev_relation = catalog ~ "." ~ dev_schema ~ "." ~ model %}
    {% set prod_relation = catalog ~ "." ~ prod_schema ~ "." ~ model %}

    {# --- 1. Row counts --- #}
    {% set count_query %}
        select
            (select count(*) from {{ dev_relation }}) as dev_rows,
            (select count(*) from {{ prod_relation }}) as prod_rows
    {% endset %}

    {% set count_result = run_query(count_query) %}
    {% set dev_rows = count_result.columns[0].values()[0] %}
    {% set prod_rows = count_result.columns[1].values()[0] %}

    {{ log("", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("DEV vs PROD COMPARISON: " ~ model, info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("Dev:  " ~ dev_relation, info=true) }}
    {{ log("Prod: " ~ prod_relation, info=true) }}
    {{ log("Key:  " ~ primary_key, info=true) }}
    {{ log("", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("ROW COUNTS", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("Dev rows:  " ~ dev_rows, info=true) }}
    {{ log("Prod rows: " ~ prod_rows, info=true) }}
    {{ log("Diff:      " ~ (dev_rows - prod_rows), info=true) }}
    {{ log("", info=true) }}

    {# --- 2. Set-level differences (rows only in one side) --- #}
    {% set only_dev_query %}
        select count(*) as cnt from {{ dev_relation }} d
        where not exists (
            select 1 from {{ prod_relation }} p
            where p.{{ primary_key }} = d.{{ primary_key }}
        )
    {% endset %}

    {% set only_prod_query %}
        select count(*) as cnt from {{ prod_relation }} p
        where not exists (
            select 1 from {{ dev_relation }} d
            where d.{{ primary_key }} = p.{{ primary_key }}
        )
    {% endset %}

    {% set only_dev_result = run_query(only_dev_query) %}
    {% set only_prod_result = run_query(only_prod_query) %}
    {% set only_dev_count = only_dev_result.columns[0].values()[0] %}
    {% set only_prod_count = only_prod_result.columns[0].values()[0] %}

    {{ log("-" * 80, info=true) }}
    {{ log("SET DIFFERENCES (by " ~ primary_key ~ ")", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("Rows only in dev:  " ~ only_dev_count, info=true) }}
    {{ log("Rows only in prod: " ~ only_prod_count, info=true) }}
    {{ log("", info=true) }}

    {# --- 3. Per-column mismatch counts for matched rows --- #}
    {% set dev_rel = adapter.get_relation(
        database=catalog, schema=dev_schema, identifier=model
    ) %}

    {% if dev_rel is none %}
        {{ log("ERROR: Could not find " ~ dev_relation, info=true) }} {{ return("") }}
    {% endif %}

    {% set columns = adapter.get_columns_in_relation(dev_rel) %}
    {% set compare_columns = [] %}
    {% for col in columns %}
        {% if col.name | lower != primary_key | lower %}
            {% do compare_columns.append(col.name) %}
        {% endif %}
    {% endfor %}

    {% set mismatch_query %}
        select
            {% for col_name in compare_columns %}
            sum(case
                when coalesce(cast(d.{{ col_name }} as string), '___NULL___')
                  != coalesce(cast(p.{{ col_name }} as string), '___NULL___')
                then 1 else 0
            end) as {{ col_name }}__mismatches
            {{ "," if not loop.last }}
            {% endfor %}
        from {{ dev_relation }} d
        inner join {{ prod_relation }} p
            on d.{{ primary_key }} = p.{{ primary_key }}
    {% endset %}

    {% set mismatch_result = run_query(mismatch_query) %}

    {{ log("-" * 80, info=true) }}
    {{ log("COLUMN-LEVEL MISMATCHES (matched rows only)", info=true) }}
    {{ log("-" * 80, info=true) }}

    {% set has_mismatches = [] %}
    {% for col_name in compare_columns %}
        {% set mismatch_count = mismatch_result.columns[loop.index0].values()[0] %}
        {% if mismatch_count > 0 %}
            {% do has_mismatches.append(col_name) %}
            {% set col_display = col_name | truncate(40, True, "...") %}
            {{
                log(
                    "  "
                    ~ col_display
                    ~ " " * (40 - col_display | length)
                    ~ " | "
                    ~ mismatch_count
                    ~ " rows differ",
                    info=true,
                )
            }}
        {% endif %}
    {% endfor %}

    {% if has_mismatches | length == 0 %}
        {{ log("  No column-level differences found for matched rows!", info=true) }}
    {% endif %}

    {{ log("", info=true) }}

    {# --- 4. Sample rows with differences --- #}
    {% if has_mismatches | length > 0 %}
        {{ log("-" * 80, info=true) }}
        {{
            log(
                "SAMPLE DIFFERENCES (first " ~ sample_size ~ " mismatched rows)",
                info=true,
            )
        }}
        {{ log("-" * 80, info=true) }}

        {% set sample_query %}
            select
                d.{{ primary_key }},
                {% for col_name in has_mismatches %}
                d.{{ col_name }} as dev__{{ col_name }},
                p.{{ col_name }} as prod__{{ col_name }}
                {{ "," if not loop.last }}
                {% endfor %}
            from {{ dev_relation }} d
            inner join {{ prod_relation }} p
                on d.{{ primary_key }} = p.{{ primary_key }}
            where
                {% for col_name in has_mismatches %}
                coalesce(cast(d.{{ col_name }} as string), '___NULL___')
                != coalesce(cast(p.{{ col_name }} as string), '___NULL___')
                {{ "or" if not loop.last }}
                {% endfor %}
            limit {{ sample_size }}
        {% endset %}

        {% set sample_result = run_query(sample_query) %}

        {% for i in range(sample_result.columns[0].values() | length) %}
            {% set pk_val = sample_result.columns[0].values()[i] %}
            {{ log("--- " ~ primary_key ~ ": " ~ pk_val ~ " ---", info=true) }}
            {% for col_name in has_mismatches %}
                {% set dev_val = sample_result.columns[loop.index0 * 2 + 1].values()[
                    i
                ] %}
                {% set prod_val = sample_result.columns[
                    loop.index0 * 2 + 2
                ].values()[i] %}
                {% if dev_val | string != prod_val | string %}
                    {{ log("  " ~ col_name ~ ":", info=true) }}
                    {{ log("    dev:  " ~ dev_val, info=true) }}
                    {{ log("    prod: " ~ prod_val, info=true) }}
                {% endif %}
            {% endfor %}
            {{ log("", info=true) }}
        {% endfor %}
    {% endif %}

    {{ log("=" * 80, info=true) }}
    {{ log("END OF COMPARISON", info=true) }}
    {{ log("=" * 80, info=true) }}

{% endmacro %}
