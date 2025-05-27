{#
    Macro: get_all_l2_columns
    Args:
        schema_name (str): The schema name to search in.
        table_names (list[str]): A list of table names to include.
    Returns:
        list[str]: A list of unique columns names for given tables
#}
{% macro get_all_l2_columns(schema_name, table_names) %}
    -- define query string
    {% set query %}
    select distinct column_name
    from system.information_schema.columns
    where table_schema = '{{ schema_name }}'
        and table_name IN ({{ "'" + table_names | join("', \n'") + "'" }})
        order by column_name
    limit 3 -- TODO: remove limit
    {% endset %}

    -- execute the query and store the results in a list
    {% set results = run_query(query) %}
    {% set columns = [] %}
    {% for result in results %}
        {% do columns.append(result.get("column_name")) %}
    {% endfor %}

    -- results depend on SQL execution, skip during parsing phase
    -- see: https://docs.getdbt.com/reference/dbt-jinja-functions/execute
    {% if execute %} {{ return(columns) }}
    {% else %} {{ return([]) }}
    {% endif %}
{% endmacro %}
