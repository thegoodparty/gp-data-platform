-- L2 packs proposed congressional maps, MI's proposed state senate, and local
-- annexation areas into one Proposed_District column, so a value is typed by
-- parsing it, never by its column name. These are the single definition of that
-- parse. The raw string shape is asserted separately, by a guard that does not
-- use these macros, so a bug here stays detectable.
{% macro is_proposed_handled_district(column_expr) -%}
    regexp_like(upper({{ column_expr }}), 'PROPOSED (CONG|STATE SEN) DIST')
{%- endmacro %}


-- Pattern -> (shadowed type, minted type). One mapping so the two macros below
-- and get_proposed_minted_district_types cannot drift apart.
{% macro proposed_district_type_map() -%}
    {{
        return(
            [
                (
                    "%PROPOSED CONG DIST%",
                    "US_Congressional_District",
                    "Congressional_District_2026",
                ),
                (
                    "%PROPOSED STATE SEN DIST%",
                    "State_Senate_District",
                    "State_Senate_District_2026",
                ),
            ]
        )
    }}
{%- endmacro %}


-- Minted type names. A voter-mart column must exist for each, or
-- m_people_api__districtvoter emits no links: it intersects voter columns with
-- district types. assert_minted_types_have_voter_columns enforces that.
{% macro get_proposed_minted_district_types() -%}
    {{ return(proposed_district_type_map() | map(attribute=2) | list) }}
{%- endmacro %}


-- The existing L2 type whose current column this value shadows, and what the
-- adoption seed records its decision against.
{% macro proposed_district_type(column_expr) -%}
    case
        {%- for pattern, shadowed, _minted in proposed_district_type_map() %}
            when upper({{ column_expr }}) like '{{ pattern }}' then '{{ shadowed }}'
        {%- endfor %}
    end
{%- endmacro %}


-- The type the district is minted as, kept separate from the shadowed one so
-- both coexist in the dimension: a campaign binds to the minted row while a
-- sitting officeholder stays on the current one.
--
-- The year is the election, not the term the map governs. Every consumer joins on
-- the election, so a 2027 label invites someone filtering election_year = 2026 to
-- miss it silently. Naming it for the term reads more accurately in isolation but
-- nothing joins on the term.
{% macro proposed_district_minted_type(column_expr) -%}
    case
        {%- for pattern, _shadowed, minted in proposed_district_type_map() %}
            when upper({{ column_expr }}) like '{{ pattern }}' then '{{ minted }}'
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
