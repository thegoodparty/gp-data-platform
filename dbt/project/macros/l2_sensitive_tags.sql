{#-
    Column-level sensitivity tags for the L2 voter file, read from the
    l2_sensitive_tags seed. Each row names an L2 column and its tag value
    (identifier or confidential). Columns not in the seed are open.

    The governed-tag job that applies these to raw tables reads this macro;
    it does not apply anything itself. Callers must declare
    -- depends_on: {{ ref("l2_sensitive_tags") }}
    because the seed is read through run_query.
-#}
{% macro l2_sensitive_tags(tag_seed="l2_sensitive_tags") %}
    {#- Returns a dict of column_name -> sensitive value. -#}
    {%- set tags = {} -%}
    {%- if execute -%}
        {%- set results = run_query(
            "select column_name, sensitive from "
            ~ ref(tag_seed)
            ~ " order by column_name"
        ) -%}
        {%- for row in results.rows -%}
            {%- do tags.update({row[0]: row[1]}) -%}
        {%- endfor -%}
    {%- endif -%}
    {{ return(tags) }}
{% endmacro %}
