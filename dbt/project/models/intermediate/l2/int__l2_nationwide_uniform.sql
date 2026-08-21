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

    assigned as (
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
    )

-- Strip L2's padding here so every consumer agrees on a district's name and id.
-- Every district column, not just the six that pad today: a no-op on the rest,
-- and nothing to keep in sync when L2 starts padding a new one.
select
    assigned.* except ({{ get_l2_district_columns() }}),
    {% for column in get_l2_district_types() -%}
        ltrim('0', assigned.`{{ column }}`) as `{{ column }}`
        {%- if not loop.last %},{% endif %}
    {% endfor %}
from assigned
