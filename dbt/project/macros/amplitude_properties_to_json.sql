{% macro amplitude_python_repr_to_json(expression) -%}
    {#- Rewrites a Python-literal string (single quotes, True/False/None) as JSON.
        Quoted strings are tokenized first so the keyword swap never touches
        string contents. -#}
    array_join(
        transform(
            regexp_extract_all(
                {{ expression }},
                '(\'(?:[^\'\\\\]|\\\\.)*\'|"(?:[^"\\\\]|\\\\.)*"|[^\'"]+)',
                1
            ),
            t -> case
                when left(t, 1) = "'"
                then
                    concat(
                        '"',
                        replace(
                            replace(substr(t, 2, length(t) - 2), "\\'", "'"), '"', '\\"'
                        ),
                        '"'
                    )
                when left(t, 1) = '"'
                then t
                else
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(t, '\\bTrue\\b', 'true'),
                            '\\bFalse\\b',
                            'false'
                        ),
                        '\\bNone\\b',
                        'null'
                    )
            end
        ),
        ''
    )
{%- endmacro %}

{% macro amplitude_properties_to_json(column) -%}
    {#- The connector lands Amplitude's JSON objects as map<string, string> with
        nested values in Python repr and booleans as True/False. Rebuild the JSON
        text Airbyte delivered so `col:key` paths keep working downstream.
        Values that look nested but do not parse stay plain strings, matching
        Airbyte (page titles like "[closed] ..."). -#}
    to_json(
        transform_values(
            {{ column }},
            (k, v) -> case
                when v is null or v = 'None'
                then cast(null as variant)
                when v = 'True'
                then cast(true as variant)
                when v = 'False'
                then cast(false as variant)
                when left(v, 1) in ('[', '{')
                then
                    coalesce(
                        try_parse_json(v),
                        try_parse_json({{ amplitude_python_repr_to_json("v") }}),
                        cast(v as variant)
                    )
                else cast(v as variant)
            end
        )
    )
{%- endmacro %}
