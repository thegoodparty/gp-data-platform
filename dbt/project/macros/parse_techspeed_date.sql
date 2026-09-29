{#
    Parse a slash-or-dash TechSpeed date. ISO values parse first. A two-component
    value tries the delivery file's detected order first, then the other order: a
    value whose day exceeds 12 only parses one way, so it lands correctly whatever
    the file-level call, and only a genuinely ambiguous value follows the file. The
    single-letter M and d tokens accept one or two digits.
#}
{% macro parse_techspeed_date(column, is_day_first) %}
    {%- set normalized = "replace(" ~ column ~ ", '/', '-')" -%}
    case
        when {{ is_day_first }}
        then
            coalesce(
                try_cast({{ normalized }} as date),
                try_to_date({{ normalized }}, 'd-M-yyyy'),
                try_to_date({{ normalized }}, 'd-M-yy'),
                try_to_date({{ normalized }}, 'M-d-yyyy'),
                try_to_date({{ normalized }}, 'M-d-yy')
            )
        else
            coalesce(
                try_cast({{ normalized }} as date),
                try_to_date({{ normalized }}, 'M-d-yyyy'),
                try_to_date({{ normalized }}, 'M-d-yy'),
                try_to_date({{ normalized }}, 'd-M-yyyy'),
                try_to_date({{ normalized }}, 'd-M-yy')
            )
    end
{% endmacro %}
