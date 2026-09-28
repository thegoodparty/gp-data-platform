-- Blunt net for null growth from any cause, where the exact-tuple tests are scoped:
-- 2.3% today, 4.1% with the L2 zero-padding drift that motivated these tests.
with
    l2_covered_types as (
        select distinct state_postal_code as state, district_type
        from {{ ref("int__l2_district_aggregations") }}
    ),

    sizeable_offices as (
        select icp.voter_count
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
    count(*) as offices,
    count(case when voter_count is null then 1 end) as unsized_offices
from sizeable_offices
having
    count(*) = 0
    or count(case when voter_count is null then 1 end) * 1.0 / nullif(count(*), 0)
    > 0.035
