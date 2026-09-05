{#
    Parse a slash-or-dash TechSpeed date, choosing the component order from the
    delivery file's detected convention. ISO values are unambiguous and parse
    first in both branches; only the two-component forms need the order. The
    single-letter M and d tokens accept one or two digits, so both padded
    (06-02-2026) and non-padded (6-2-2026) values parse.
#}
{% macro parse_techspeed_date(column, is_day_first) %}
    case
        when {{ is_day_first }}
        then
            coalesce(
                try_cast(replace({{ column }}, '/', '-') as date),
                try_to_date(replace({{ column }}, '/', '-'), 'd-M-yyyy'),
                try_to_date(replace({{ column }}, '/', '-'), 'd-M-yy')
            )
        else
            coalesce(
                try_cast(replace({{ column }}, '/', '-') as date),
                try_to_date(replace({{ column }}, '/', '-'), 'M-d-yyyy'),
                try_to_date(replace({{ column }}, '/', '-'), 'M-d-yy')
            )
    end
{% endmacro %}
