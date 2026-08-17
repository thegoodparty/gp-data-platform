-- A Proposed_District row in the District dimension is bindable, so only an
-- adopted and checked congressional map may appear. Anything else reaching this
-- mart (an unverified state, a state seeded current or needs_boundary, a
-- district the state has not rolled out yet, or a non-congressional proposed
-- value such as MI's state senate or a WA/CO annexation) means the gate in
-- m_election_api__district stopped working.
with
    adopted as (
        select state, cast(nullif(trim(district_number), '') as int) as district_number
        from {{ ref("district_map_adoption") }}
        where
            district_type = 'US_Congressional_District'
            and adopted_source = 'proposed'
            and is_verified
    )

select district.state, district.l2_district_type, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
where
    district.l2_district_type = 'Proposed_District'
    and not exists (
        select 1
        from adopted
        where
            adopted.state = district.state
            and {{ is_proposed_cong_dist("district.l2_district_name") }}
            and (
                adopted.district_number is null
                or adopted.district_number
                = {{ proposed_cong_dist_number("district.l2_district_name") }}
            )
    )
