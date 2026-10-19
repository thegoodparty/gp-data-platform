{#-
    Accessor over the census_acs_b01001_cells seed: B01001's 49 cells with
    the column name staging gives each. stg_census_acs__b01001 generates its
    columns from it, so a cell's name and its Census label sit on one row.
    Models calling it read the seed via run_query, so they must declare:
    -- depends_on: {{ ref("census_acs_b01001_cells") }}
-#}
{% macro census_acs_b01001_cells(seed="census_acs_b01001_cells") %}
    {#- (cell, column_name) pairs in cell order. Empty at parse time
        (execute=false). -#}
    {%- set cells = [] -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select cell, column_name from " ~ ref(seed) ~ " order by cell"
        ) -%}
        {%- for row in results.rows -%}
            {%- do cells.append((row[0] | int, row[1])) -%}
        {%- endfor -%}
        {%- if cells | length != 49 -%}
            {{
                exceptions.raise_compiler_error(
                    "census_acs_b01001_cells: expected 49 B01001 cells, found "
                    ~ (cells | length)
                )
            }}
        {%- endif -%}
    {%- endif -%}
    {{ return(cells) }}
{% endmacro %}
