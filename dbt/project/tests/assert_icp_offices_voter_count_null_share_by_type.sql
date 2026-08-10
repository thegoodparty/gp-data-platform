-- Where the signal is: a district-name break concentrates in one type, so the share
-- there jumps far past the ~29% some types run legitimately (School_District 16%,
-- Multi_township_Assessor 29%). The L2 zero-padding drift put State_House at 73%.
with
    l2_covered_types as (
        select distinct state_postal_code as state, district_type
        from {{ ref("int__l2_district_aggregations") }}
    ),

    sizeable_offices as (
        select icp.l2_district_type, icp.voter_count
        from {{ ref("int__icp_offices") }} as icp
        join
            l2_covered_types
            on l2_covered_types.state = icp.state
            and l2_covered_types.district_type = icp.l2_district_type
        where
            icp.is_matched
            and not coalesce(icp.is_judicial, false)
            and not coalesce(icp.is_appointed, false)
    )

select
    l2_district_type,
    count(*) as offices,
    count(case when voter_count is null then 1 end) as unsized_offices
from sizeable_offices
group by l2_district_type
having
    count(*) >= 100
    and count(case when voter_count is null then 1 end) * 1.0 / count(*) > 0.35
