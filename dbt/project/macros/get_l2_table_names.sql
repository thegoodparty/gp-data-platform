{#
    Macro: get_l2_table_names
    Args:
        schema_name (str): The name of the schema to search in.
        table_prefix (str): The prefix of the tables to search for.
        table_suffix (str): The suffix of the tables to search for.
    Returns:
        list[str]: A list of table names matching the prefix in the given schema.
#}
{% macro get_l2_table_names(schema_name, table_prefix, table_suffix) %}
    {% set query %}
        SELECT table_name
        FROM system.information_schema.tables
        WHERE table_schema = '{{ schema_name }}'
        AND table_name LIKE '{{ table_prefix }}%'
        AND table_name LIKE '%{{ table_suffix }}'
    {% endset %}
    {{ return(run_query(query).columns[0].values()) }}
{% endmacro %}
