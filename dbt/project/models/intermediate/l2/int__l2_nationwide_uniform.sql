-- To assign a district L2 does not carry, add rows to
-- l2_manual_district_assignments. L2 stays authoritative wherever it supplies a
-- value.
with
    assigned as (
        select
            voters.* except (`City_Council_Commissioner_District`),
            coalesce(
                voters.`City_Council_Commissioner_District`,
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
    )

-- Strip L2's padding here so every consumer agrees on a district's name and id.
select
    assigned.* except ({{ get_l2_district_columns() }}),
    {{ strip_l2_district_zero_padding_projection("assigned") }}
from assigned
