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
    -- define query string
    {% set query %}
        select table_name
        from system.information_schema.tables
        where
            table_schema = '{{ schema_name }}'
            and table_name like '{{ table_prefix }}%'
            and table_name like '%{{ table_suffix }}'
        limit 5 -- TODO: remove limit
    {% endset %}

    -- execute the query and store the results in a list
    {% set results = run_query(query) %}
    {% set table_names = [] %}
    {% for result in results %}
        {% do table_names.append(result.get("table_name")) %}
    {% endfor %}

    -- results depend on SQL execution, skip during parsing phase
    -- see: https://docs.getdbt.com/reference/dbt-jinja-functions/execute
    {% if execute %} {{ return(table_names) }}
    {% else %} {{ return([]) }}
    {% endif %}
{% endmacro %}
