{#
    Macro: get_all_columns
    Args:
        schema_name (str): The schema name to search in.
        table_names (list[str]): A list of table names to include.
    Returns:
        list[str]: A list of column names.
#}
{% macro get_all_columns(schema_name, table_names) %}
    {% set query %}
        SELECT DISTINCT column_name
        FROM system.information_schema.columns
        WHERE table_schema = '{{ schema_name }}'
        AND table_name IN ({{ "'" + table_names | join("', \n'") + "'" }})
        ORDER BY column_name
    {% endset %}
    {{ return(run_query(query).columns[0].values()) }}
{% endmacro %}
