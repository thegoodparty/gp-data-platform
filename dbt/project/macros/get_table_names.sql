{#
    Macro: get_table_names
    Args:
        schema_name (str): The name of the schema to search in.
        table_prefix (str): The prefix of the tables to search for.
    Returns:
        list[str]: A list of table names matching the prefix in the given schema.
#}
{% macro get_table_names(schema_name, table_prefix) %}
    {% set query %}
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_schema = '{{ schema_name }}'
        AND table_name LIKE '{{ table_prefix }}%'
    {% endset %}
    {{ return(run_query(query).columns[0].values()) }}
{% endmacro %}
