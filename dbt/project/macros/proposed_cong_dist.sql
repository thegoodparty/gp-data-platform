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
-- Models share this. The assertion guarding the gate restates the *seed lookup*
-- by hand, so a mistake in which rows count as adopted cannot make the guard
-- agree with the gate. It does share the two parse macros above, so a mistake in
-- the parse propagates into the guard — that shape is covered instead by
-- assert_proposed_cong_dist_format, which reads the vendor column directly and
-- deliberately does not go through these macros.
{% macro is_adopted_proposed_congressional(state_expr, name_expr) -%}
    {#-
        The outer references below sit inside a correlated subquery over the
        adoption seed, and an unqualified name binds to the innermost scope
        first. So passing a bare `state` silently resolves to the seed's own
        state column, turning the correlation into `adoption.state =
        adoption.state` — always true, gate wide open, no error anywhere. That
        has happened twice. Refuse it at compile time rather than trusting the
        next caller to remember.
    -#}
    {%- set seed_columns = [
        "state",
        "district_type",
        "district_number",
        "adopted_source",
        "is_verified",
        "source_url",
        "notes",
    ] -%}
    {%- for expr in [state_expr, name_expr] -%}
        {%- if "." not in expr and expr | trim | lower in seed_columns -%}
            {{
                exceptions.raise_compiler_error(
                    "is_adopted_proposed_congressional got '"
                    ~ expr
                    ~ "', which collides with a district_map_adoption column. "
                    ~ "Qualify it with its table alias, or the correlation "
                    ~ "becomes a tautology and the gate stops gating."
                )
            }}
        {%- endif -%}
    {%- endfor -%} exists (
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


-- Post-hook for the district mart. Its merge only adds and updates, so the gate
-- can admit a district but never take one back: rows minted while a state was
-- adopted survive a seed edit that would no longer admit them. Retraction is the
-- case this design exists for — a court ruling striking a map down is exactly
-- what happened to Virginia — so without this a flip leaves stale bindable
-- districts behind and reds the gate assertion until someone knows to run a full
-- refresh, under time pressure. This makes the seed authoritative both ways.
-- The alias is load-bearing: district_map_adoption has its own `state` column,
-- so an unqualified `state` inside the correlated subquery binds to the seed
-- rather than to the target, making the correlation a tautology.
{% macro retract_unadopted_proposed_districts() -%}
    delete from {{ this }} as district
    where
        district.l2_district_type = 'Proposed_District' and not
        {{
            is_adopted_proposed_congressional(
                "district.state", "district.l2_district_name"
            )
        }}
{%- endmacro %}


-- A Proposed_District row is worth carrying only if it is the adopted map.
-- Everything else in that column — states seeded current or needs_boundary, MI's
-- proposed state senate, CO/WA annexation areas — is unbindable, so aggregating
-- it at voter grain is work whose result nothing can ever read.
--
-- A case expression rather than `type != '...' or exists (...)`, for two
-- reasons. It is a single boolean, so a caller can safely append a further
-- condition — the bare or form binds as `(x and type != ...) or exists (...)`,
-- quietly dropping every other conjunct for rows the exists matches. And the
-- subquery is unreachable for non-proposed rows, so it is not evaluated across
-- the whole unpivoted voter grain the way a disjunction forces.
{% macro retain_district_row(type_expr, state_expr, name_expr) -%}
    case
        when {{ type_expr }} = 'Proposed_District'
        then {{ is_adopted_proposed_congressional(state_expr, name_expr) }}
        else true
    end
{%- endmacro %}
