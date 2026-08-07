{% macro strip_l2_district_zero_padding(column_expr) %}
    {#- L2 pads district numbers ('023'); the BR match snapshot and turnout feeds
        do not, so name-keyed joins miss and the salted id mints two districts. -#}
    ltrim('0', {{ column_expr }})
{%- endmacro %}


{% macro strip_l2_district_zero_padding_projection(relation_alias) %}
    {#- Pair with `select <alias>.* except ({{ get_l2_district_columns() }})`.
        Applied to every district column, not just the six that pad today: a
        no-op elsewhere, and nothing to keep in sync when L2 pads a new one. -#}
    {%- set out = [] -%}
    {%- for col in get_l2_district_columns(use_backticks=false).split(",") -%}
        {%- set name = col | trim -%}
        {%- set _ = out.append(
            strip_l2_district_zero_padding(
                relation_alias ~ ".`" ~ name ~ "`"
            )
            | trim ~ " as `" ~ name ~ "`"
        ) -%}
    {%- endfor -%}
    {{ out | join(",\n    ") }}
{% endmacro %}
