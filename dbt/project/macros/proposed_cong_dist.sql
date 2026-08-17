-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value has to be
-- typed by parsing it and never by its column name. These two macros are the
-- single definition of that parse: the token that separates a congressional
-- value from everything else in the column, and the district number inside it.
-- The gate that admits a proposed district and the assertion guarding that gate
-- both parse through here, so a vendor format change can never leave them
-- disagreeing about what a name means. The raw string shape is asserted
-- separately and deliberately does not use these macros.
{% macro is_proposed_cong_dist(column_expr) -%}
    upper({{ column_expr }}) like '%PROPOSED CONG DIST%'
{%- endmacro %}

{% macro proposed_cong_dist_number(column_expr) -%}
    cast(
        regexp_extract(
            upper({{ column_expr }}), '^[0-9]{4} PROPOSED CONG DIST ([0-9]+)', 1
        ) as int
    )
{%- endmacro %}
