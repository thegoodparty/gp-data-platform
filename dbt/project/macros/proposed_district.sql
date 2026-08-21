-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value has to be typed
-- by parsing it and never by its column name. These are the single definition of
-- which values are handled, which L2 type each one shadows, the type it is minted
-- as, and the district number inside it.
--
-- The raw string shape is asserted separately, by a guard that deliberately does
-- not use these macros, so that a bug here stays detectable.
{% macro is_proposed_handled_district(column_expr) -%}
    regexp_like(upper({{ column_expr }}), 'PROPOSED (CONG|STATE SEN) DIST')
{%- endmacro %}


-- The existing L2 type whose current column this value shadows. This is what the
-- adoption seed records a decision against, so the seed join and the coverage
-- guard both go through here.
{% macro proposed_district_type(column_expr) -%}
    case
        when upper({{ column_expr }}) like '%PROPOSED CONG DIST%'
        then 'US_Congressional_District'
        when upper({{ column_expr }}) like '%PROPOSED STATE SEN DIST%'
        then 'State_Senate_District'
    end
{%- endmacro %}


-- The type the district is minted as. Separate from the shadowed type so both
-- can coexist in the dimension: a campaign binds to the minted row while a
-- sitting officeholder stays on the current one. Year is the election year, not
-- the term year, because every consumer joins on the election.
{% macro proposed_district_minted_type(column_expr) -%}
    case
        when upper({{ column_expr }}) like '%PROPOSED CONG DIST%'
        then 'Congressional_District_2026'
        when upper({{ column_expr }}) like '%PROPOSED STATE SEN DIST%'
        then 'State_Senate_District_2026'
    end
{%- endmacro %}


-- District number as a bare unpadded string, matching how every current column
-- names its districts. The int cast is what strips the vendor's leading zero, so
-- "DIST 04" becomes "4" rather than "04" and lines up with both the existing
-- convention and BallotReady's numbering.
{% macro proposed_district_number(column_expr) -%}
    cast(
        cast(
            regexp_extract(
                upper({{ column_expr }}), 'PROPOSED (?:CONG|STATE SEN) DIST ([0-9]+)', 1
            ) as int
        ) as string
    )
{%- endmacro %}
