-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value is typed by
-- parsing it, never by its column name. These are the single definition of that
-- parse. The raw string shape is asserted separately, by a guard that does not
-- use these macros, so a bug here stays detectable.
{% macro is_proposed_handled_district(column_expr) -%}
    regexp_like(upper({{ column_expr }}), 'PROPOSED (CONG|STATE SEN) DIST')
{%- endmacro %}


-- Pattern -> the current type an adopted proposed map replaces.
{% macro proposed_district_type_map() -%}
    {{
        return(
            [
                ("%PROPOSED CONG DIST%", "US_Congressional_District"),
                ("%PROPOSED STATE SEN DIST%", "State_Senate_District"),
            ]
        )
    }}
{%- endmacro %}


-- The current type this value replaces, and what the adoption seed records its
-- decision against.
{% macro proposed_district_type(column_expr) -%}
    case
        {%- for pattern, replaced in proposed_district_type_map() %}
            when upper({{ column_expr }}) like '{{ pattern }}' then '{{ replaced }}'
        {%- endfor %}
    end
{%- endmacro %}


-- District number as a bare unpadded string, matching how every current column
-- names its districts. The int cast strips the vendor's leading zero, so
-- "DIST 04" becomes "4" and lines up with BallotReady's numbering.
{% macro proposed_district_number(column_expr) -%}
    cast(
        cast(
            regexp_extract(
                upper({{ column_expr }}), 'PROPOSED (?:CONG|STATE SEN) DIST ([0-9]+)', 1
            ) as int
        ) as string
    )
{%- endmacro %}
