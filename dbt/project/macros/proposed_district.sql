-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value has to be typed
-- by parsing it and never by its column name. These are the single definition of
-- which values are handled and which L2 type each one belongs to.
--
-- The raw string shape is asserted separately, by a guard that deliberately does
-- not use these macros, so that a bug here stays detectable.
{% macro is_proposed_handled_district(column_expr) -%}
    regexp_like(upper({{ column_expr }}), 'PROPOSED (CONG|STATE SEN) DIST')
{%- endmacro %}


-- Maps a handled value to the L2 district type whose current column it shadows.
-- Null for anything unhandled, so a caller filtering on this is scoped to what
-- the design covers without restating the token list.
{% macro proposed_district_type(column_expr) -%}
    case
        when upper({{ column_expr }}) like '%PROPOSED CONG DIST%'
        then 'US_Congressional_District'
        when upper({{ column_expr }}) like '%PROPOSED STATE SEN DIST%'
        then 'State_Senate_District'
    end
{%- endmacro %}
