{% macro sanitize_l2_phone_number(column) %}
    {#-
        Keep a phone value only when it holds a full 10-digit number once
        non-digits are stripped; every other shape (partial, too long, blank,
        non-numeric, null) becomes null. Presence counts downstream then reflect
        dialable numbers instead of counting junk values.
    -#}
    case
        when length(regexp_replace(cast({{ column }} as string), '[^0-9]', '')) = 10
        then {{ column }}
    end
{% endmacro %}
