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


-- True when a Proposed_District value is the map a state actually runs under.
-- Self-contained so a consumer needs no companion CTE: the seed is a handful of
-- rows, so the correlated lookup costs nothing. A null district_number means the
-- decision covers the whole state; a value narrows it to that one district, so a
-- state can be rolled out district by district as each is checked against the
-- enacted map.
--
-- Models share this; the assertion guarding the gate deliberately restates the
-- rule by hand, so a mistake here cannot make the guard agree with it.
{% macro is_adopted_proposed_congressional(state_expr, name_expr) -%}
    exists (
        select 1
        from {{ ref("district_map_adoption") }} as adoption
        where
            adoption.district_type = 'US_Congressional_District'
            and adoption.adopted_source = 'proposed'
            and adoption.is_verified
            and adoption.state = {{ state_expr }}
            and {{ is_proposed_cong_dist(name_expr) }}
            and (
                cast(nullif(trim(adoption.district_number), '') as int) is null
                or cast(nullif(trim(adoption.district_number), '') as int)
                = {{ proposed_cong_dist_number(name_expr) }}
            )
    )
{%- endmacro %}


-- A Proposed_District row is worth carrying only if it is the adopted map.
-- Everything else in that column — states seeded current or needs_boundary, MI's
-- proposed state senate, CO/WA annexation areas — is unbindable, so aggregating
-- it at voter grain is work whose result nothing can ever read.
{% macro retain_district_row(type_expr, state_expr, name_expr) -%}
    {{ type_expr }} != 'Proposed_District'
    or {{ is_adopted_proposed_congressional(state_expr, name_expr) }}
{%- endmacro %}
