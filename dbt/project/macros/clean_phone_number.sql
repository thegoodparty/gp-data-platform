{% macro clean_phone_number(column) %}
    nullif(regexp_replace({{ column }}, '[^0-9]', ''), '')
{% endmacro %}


{% macro valid_phone_number(column) %}
    {#- Keeps the original value only when its digits form a 10-digit US
        number; anything else would reach outreach tools or inflate
        presence counts, so it becomes null. -#}
    case when length({{ clean_phone_number(column) }}) = 10 then {{ column }} end
{% endmacro %}
