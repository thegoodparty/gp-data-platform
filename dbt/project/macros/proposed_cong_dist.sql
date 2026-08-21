-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value has to be typed
-- by parsing it and never by its column name. This is the single definition of
-- the token that separates a congressional value from the rest of the column.
--
-- The raw string shape is asserted separately, by a guard that deliberately does
-- not use this macro, so that a bug here stays detectable.
{% macro is_proposed_cong_dist(column_expr) -%}
    upper({{ column_expr }}) like '%PROPOSED CONG DIST%'
{%- endmacro %}
