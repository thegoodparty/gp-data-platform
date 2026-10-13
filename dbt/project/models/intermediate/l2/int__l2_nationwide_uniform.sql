-- To assign a district L2 does not carry, add rows to
-- l2_manual_district_assignments. L2 stays authoritative wherever it supplies a
-- value. Each district type the seed uses needs its own coalesce block below;
-- a type without one never reaches voters, and
-- assert_manual_district_assignments_resolve fails until it gets one.
with
    council_assigned as (
        select
            voters.* except (`City_Council_Commissioner_District`),
            -- nullif: some state files load blanks as '' rather than null; a blank
            -- is absence, not an L2-supplied value, so it must not beat the seed.
            coalesce(
                nullif(voters.`City_Council_Commissioner_District`, ''),
                assignments.l2_district_name
            ) as `City_Council_Commissioner_District`
        from {{ ref("int__l2_nationwide_uniform_raw_districts") }} as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'City_Council_Commissioner_District'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    hospital_assigned as (
        select
            voters.* except (`Hospital_District`),
            coalesce(
                nullif(voters.`Hospital_District`, ''), assignments.l2_district_name
            ) as `Hospital_District`
        from council_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Hospital_District'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    circuit_assigned as (
        select
            voters.* except (`Judicial_Circuit_Court_District`),
            coalesce(
                nullif(voters.`Judicial_Circuit_Court_District`, ''),
                assignments.l2_district_name
            ) as `Judicial_Circuit_Court_District`
        from hospital_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Judicial_Circuit_Court_District'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    -- Louisiana's court column carries the judgeship election sections of
    -- R.S. 13:477, not whole judicial districts, and leaves Judicial_District
    -- empty statewide. A district attorney is elected by the whole district,
    -- so parish rows mint it here without touching the judge sections.
    judicial_district_assigned as (
        select
            voters.* except (`Judicial_District`),
            coalesce(
                nullif(voters.`Judicial_District`, ''), assignments.l2_district_name
            ) as `Judicial_District`
        from circuit_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Judicial_District'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    -- Louisiana city courts draw their electorate from parish wards by statute,
    -- so a court spanning several wards has no single L2 district to match.
    magistrate_assigned as (
        select
            voters.* except (`Judicial_Magistrate_Division`),
            coalesce(
                nullif(voters.`Judicial_Magistrate_Division`, ''),
                assignments.l2_district_name
            ) as `Judicial_Magistrate_Division`
        from judicial_district_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Judicial_Magistrate_Division'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    -- Metro Nashville's consolidated government fills every Davidson County
    -- voter's City, so L2 carries the county's satellite cities in Town_District.
    -- A satellite that also spans a neighboring county is split between the two
    -- columns; giving the neighbor's voters the same Town_District reunites it.
    town_assigned as (
        select
            voters.* except (`Town_District`),
            coalesce(
                nullif(voters.`Town_District`, ''), assignments.l2_district_name
            ) as `Town_District`
        from magistrate_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Town_District'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
    ),

    -- A trustee area subdivides one school district, but a county precinct can
    -- straddle two districts, so an assigned sub-district only applies to
    -- voters of the parent it names (L2 spells them '<parent> TA <n>').
    assigned as (
        select
            voters.* except (`Unified_School_SubDistrict`),
            coalesce(
                nullif(voters.`Unified_School_SubDistrict`, ''),
                assignments.l2_district_name
            ) as `Unified_School_SubDistrict`
        from town_assigned as voters
        left join
            {{ ref("l2_manual_district_assignments") }} as assignments
            on assignments.l2_district_type = 'Unified_School_SubDistrict'
            and assignments.state = voters.state_postal_code
            and (assignments.county is null or assignments.county = voters.county)
            and (assignments.city is null or assignments.city = voters.city)
            and (assignments.precinct is null or assignments.precinct = voters.precinct)
            and startswith(
                assignments.l2_district_name, voters.`Unified_School_District` || ' '
            )
    ),

    -- Statewide adoptions, where a voter L2 left off the proposed map has no
    -- district rather than a number from the map it replaced.
    statewide_adoptions as (
        select concat(state, '|', district_type) as scope
        from {{ ref("district_map_adoption") }}
        where
            adopted_source = 'proposed'
            and is_verified
            and nullif(trim(district_number), '') is null
    ),

    -- Where the adoption seed puts a state on its proposed map, that map replaces
    -- the current column outright, so every consumer sees one map per type.
    -- Virginia's struck-down proposal never passes the gate.
    {% set replaced_types = proposed_district_type_map() | map(attribute=1) | list %}
    adopted as (
        select
            voters.* except (
                {% for column in replaced_types -%}
                    `{{ column }}`{% if not loop.last %},{% endif %}
                {%- endfor %}
            ),
            {% for column in replaced_types -%}
                case
                    when adoption.district_type = '{{ column }}'
                    then {{ proposed_district_number("voters.`Proposed_District`") }}
                    when
                        concat(voters.state_postal_code, '|{{ column }}')
                        in (select scope from statewide_adoptions)
                    then null
                    else voters.`{{ column }}`
                end as `{{ column }}`
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from assigned as voters
        left join
            {{ ref("district_map_adoption") }} as adoption
            on adoption.state = voters.state_postal_code
            and adoption.district_type
            = {{ proposed_district_type("voters.`Proposed_District`") }}
            and adoption.adopted_source = 'proposed'
            and adoption.is_verified
            -- An empty district_number covers the whole state.
            and (
                nullif(trim(adoption.district_number), '') is null
                or cast(trim(adoption.district_number) as int) = cast(
                    {{ proposed_district_number("voters.`Proposed_District`") }} as int
                )
            )
    )

-- Strip L2's padding here so every consumer agrees on a district's name and id.
-- Every district column, not just the six that pad today: a no-op on the rest,
-- and nothing to keep in sync when L2 starts padding a new one.
select
    adopted.* except (
        {{ get_l2_district_columns() }},
        `VoterTelephones_CellPhoneFormatted`,
        `VoterTelephones_LandlineFormatted`
    ),
    {% for column in get_l2_district_types() -%}
        ltrim('0', adopted.`{{ column }}`) as `{{ column }}`,
    {% endfor %}
    {{ clean_l2_phone_number("adopted.`VoterTelephones_CellPhoneFormatted`") }}
    as `VoterTelephones_CellPhoneFormatted`,
    {{ clean_l2_phone_number("adopted.`VoterTelephones_LandlineFormatted`") }}
    as `VoterTelephones_LandlineFormatted`
from adopted
