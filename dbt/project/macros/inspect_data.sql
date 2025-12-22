{% macro inspect_data(model=none, source_name=none, table_name=none, sample_size=5) %}
    {#
        Utility macro for AI tools to inspect dbt models and sources.

        Usage:
            For models:
                dbt run-operation inspect_data --args '{"model": "my_model_name"}'

            For sources:
                dbt run-operation inspect_data --args '{"source_name": "airbyte_source", "table_name": "gp_api_db_campaign"}'

        Parameters:
            model: Name of the dbt model (without ref())
            source_name: Name of the source (for sources only)
            table_name: Name of the table within the source (for sources only)
            sample_size: Number of sample rows to return (default: 5)

        Returns (via log output):
            - Relation name and type
            - Total row count
            - Column information with data types
            - Non-null count and population percentage for each column
            - Sample rows

        Note: If a model doesn't exist in your dev environment, run
        `dbt run --select model_name` first to materialize it.
    #}
    {# Determine the relation based on parameters #}
    {% if model is not none %}
        {% set relation = ref(model) %}
        {% set relation_type = "model" %}
        {% set relation_display_name = model %}
    {% elif source_name is not none and table_name is not none %}
        {% set relation = source(source_name, table_name) %}
        {% set relation_type = "source" %}
        {% set relation_display_name = source_name ~ "." ~ table_name %}
    {% else %}
        {{
            log(
                "ERROR: Must provide either 'model' OR both 'source_name' and 'table_name'",
                info=true,
            )
        }}
        {{ return("") }}
    {% endif %}

    {# Get column information #}
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% if columns | length == 0 %}
        {{
            log(
                "ERROR: Could not retrieve columns for relation. The table may not exist or may be empty.",
                info=true,
            )
        }}
        {{ return("") }}
    {% endif %}

    {# Build and run row count query #}
    {% set row_count_query %}
        select count(*) as row_count from {{ relation }}
    {% endset %}

    {% set row_count_result = run_query(row_count_query) %}
    {% set row_count = row_count_result.columns[0].values()[0] %}

    {# Build and run column population query #}
    {% set population_query %}
        select
            {{ row_count }} as total_rows
            {% for column in columns %}
            , count({{ adapter.quote(column.name) }}) as {{ adapter.quote(column.name ~ '_non_null_count') }}
            {% endfor %}
        from {{ relation }}
    {% endset %}

    {% set population_result = run_query(population_query) %}

    {# Build and run sample query #}
    {% set sample_query %}
        select * from {{ relation }} limit {{ sample_size }}
    {% endset %}

    {% set sample_result = run_query(sample_query) %}

    {# Output the results #}
    {{ log("", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("DATA INSPECTION REPORT", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("", info=true) }}
    {{ log("Relation: " ~ relation, info=true) }}
    {{ log("Type: " ~ relation_type, info=true) }}
    {{ log("Name: " ~ relation_display_name, info=true) }}
    {{ log("", info=true) }}

    {{ log("-" * 80, info=true) }}
    {{ log("ROW COUNT", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("Total rows: " ~ row_count | string, info=true) }}
    {{ log("", info=true) }}

    {{ log("-" * 80, info=true) }}
    {{ log("COLUMN DETAILS", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("", info=true) }}

    {# Column header #}
    {% set col_name_header = "Column Name" | truncate(40, True, "...") %}
    {% set col_type_header = "Data Type" | truncate(20, True, "...") %}
    {% set non_null_header = "Non-Null" | truncate(12, True, "...") %}
    {% set pct_header = "% Populated" | truncate(12, True, "...") %}
    {{
        log(
            "| "
            ~ col_name_header
            ~ " " * (40 - col_name_header | length)
            ~ " | "
            ~ col_type_header
            ~ " " * (20 - col_type_header | length)
            ~ " | "
            ~ non_null_header
            ~ " " * (12 - non_null_header | length)
            ~ " | "
            ~ pct_header
            ~ " " * (12 - pct_header | length)
            ~ " |",
            info=true,
        )
    }}
    {{
        log(
            "|" ~ "-" * 42 ~ "|" ~ "-" * 22 ~ "|" ~ "-" * 14 ~ "|" ~ "-" * 14 ~ "|",
            info=true,
        )
    }}

    {% for column in columns %}
        {% set col_index = loop.index %}
        {% set non_null_count = population_result.columns[col_index].values()[0] %}
        {% if row_count > 0 %}
            {% set pct_populated = ((non_null_count / row_count) * 100) | round(1) %}
        {% else %} {% set pct_populated = 0 %}
        {% endif %}

        {% set col_name_display = column.name | truncate(40, True, "...") %}
        {% set col_type_display = column.data_type | truncate(20, True, "...") %}
        {% set non_null_display = non_null_count | string %}
        {% set pct_display = pct_populated | string ~ "%" %}

        {{
            log(
                "| "
                ~ col_name_display
                ~ " " * (40 - col_name_display | length)
                ~ " | "
                ~ col_type_display
                ~ " " * (20 - col_type_display | length)
                ~ " | "
                ~ non_null_display
                ~ " " * (12 - non_null_display | length)
                ~ " | "
                ~ pct_display
                ~ " " * (12 - pct_display | length)
                ~ " |",
                info=true,
            )
        }}
    {% endfor %}

    {{ log("", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("SAMPLE DATA (" ~ sample_size ~ " rows)", info=true) }}
    {{ log("-" * 80, info=true) }}
    {{ log("", info=true) }}

    {# Print column headers for sample data #}
    {% set header_parts = [] %}
    {% for column in columns %}
        {% do header_parts.append(column.name | truncate(25, True, "...")) %}
    {% endfor %}
    {{ log("Columns: " ~ header_parts | join(" | "), info=true) }}
    {{ log("", info=true) }}

    {# Print sample rows #}
    {% for i in range(sample_result.columns[0].values() | length) %}
        {% set row_parts = [] %}
        {% for column in columns %}
            {% set col_idx = loop.index0 %}
            {% set cell_value = sample_result.columns[col_idx].values()[i] %}
            {% if cell_value is none %} {% set cell_display = "NULL" %}
            {% else %}
                {% set cell_display = cell_value | string | truncate(25, True, "...") %}
            {% endif %}
            {% do row_parts.append(cell_display) %}
        {% endfor %}
        {{ log("Row " ~ (i + 1) ~ ": " ~ row_parts | join(" | "), info=true) }}
    {% endfor %}

    {{ log("", info=true) }}
    {{ log("=" * 80, info=true) }}
    {{ log("END OF REPORT", info=true) }}
    {{ log("=" * 80, info=true) }}

{% endmacro %}
