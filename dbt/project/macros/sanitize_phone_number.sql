{#-
    Keeps a phone number only when its digits form a 10-digit US number once
    non-digits are stripped; anything else (partial, over-long, blank,
    non-numeric, null) becomes null. Downstream presence counts and end-user
    exports should reflect dialable numbers, not raw vendor strings.
    The fragment columns (area code, 7-digit) are intentionally not run
    through this: they are parts of a number, so a full-number rule would
    null them out entirely.
-#}
{% macro sanitize_phone_number(column) %}
    case
        when length(regexp_replace({{ column }}, '[^0-9]', '')) = 10 then {{ column }}
    end
{% endmacro %}
