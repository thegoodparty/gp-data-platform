{% macro strip_ts_stage_suffix(column) -%}
    regexp_replace({{ column }}, '__(primary|general|runoff)$', '')
{%- endmacro %}
