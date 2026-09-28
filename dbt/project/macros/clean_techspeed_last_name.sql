{#
    TechSpeed surnames arrive with generational suffixes, a trailing comma the
    suffix leaves behind, and middle initials merged in ("A. Smith", "Smith J").
    Lookarounds keep compound surnames ("De La Cruz", "St. John") intact. Used by
    the valid and invalid staging models so both compare the same surname.
#}
{% macro clean_techspeed_last_name(column) %}
    regexp_replace(
        regexp_replace(
            regexp_replace(
                {{ remove_name_suffixes("trim(" ~ column ~ ")") }}, ',$', ''
            ),
            '^([A-Z][.] ?|[A-Z] )+(?=[A-Za-z])',
            ''
        ),
        '(?<=[A-Za-z]) [A-Z]$',
        ''
    )
{% endmacro %}
