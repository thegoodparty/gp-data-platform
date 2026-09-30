{% macro clean_l2_phone_number(column) %}
    {#- Anything that is not a 10-digit NANP number becomes null, so a non-null
    value means a usable phone. Valid numbers keep L2's (999) 999-9999 form,
    which gp-api exports as-is. -#}
    regexp_replace(
        nullif(
            regexp_extract(
                {{ clean_phone_number(column) }}, '^([2-9][0-9]{2}[2-9][0-9]{6})$', 1
            ),
            ''
        ),
        '^([0-9]{3})([0-9]{3})([0-9]{4})$',
        '($1) $2-$3'
    )
{% endmacro %}
