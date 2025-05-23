{% set source_schema_name = "dbt" %}
{% set table_prefix = "stg_dbt_source__l2_s3_" %}
{% set table_suffix = "_vote_history" %}

-- Macro to get table names (defined separately)
{% set table_names = (
    run_query(
        get_l2_table_names(source_schema_name, table_prefix, table_suffix)
    )
    | list
) %}

-- Get all unique columns across tables
{% set all_columns = (
    run_query(get_all_columns(source_schema_name, table_names)) | list
) %}

select
    {% for col in all_columns %}
        {{ col }} {% if not loop.last %}, {% endif %}
    {% endfor %}
from
    (
        {% for table in table_names %}
            select
                {% for col in all_columns %}
                    {% if col in get_table_columns(schema_name, table) %} {{ col }}
                    {% else %} null as {{ col }}
                    {% endif %}
                    {% if not loop.last %}, {% endif %}
                {% endfor %}
            from {{ schema_name }}.{{ table }}
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ) as unioned_tables
