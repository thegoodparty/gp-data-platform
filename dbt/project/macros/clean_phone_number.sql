{% macro clean_phone_number(column) %}
    nullif(regexp_replace({{ column }}, '[^0-9]', ''), '')
{% endmacro %}
