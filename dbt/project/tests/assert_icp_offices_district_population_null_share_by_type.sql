-- Coverage canary for district_population, the population twin of
-- assert_icp_offices_voter_count_null_share_by_type. A band canary, not a fixed count.
--
-- Why per type rather than an overall coverage rate: a join break concentrates in one
-- district type instead of spreading evenly, so the aggregate hides it. The L2
-- zero-padding drift moved overall coverage by under two points while pushing
-- State_House_District's null share to 73%, which is invisible in a headline number and
-- glaring here.
--
-- Scoped to (state, type) pairs the substrate actually covers. Types outside the
-- curated substrate subset are 100% null by design, not by breakage, so including them
-- would make the test meaningless.
--
-- Threshold: 35%. The worst covered type today is City_School_District at 13.9% over 41
-- qualifying types, so this clears real drift while still tripping on a padding-class
-- regression. Raise it only with the measurement that justifies it.
with
    substrate_covered as (
        select distinct state_postal_code as state, district_type
        from {{ ref("int__district_population") }}
    ),

    sizeable_offices as (
        select icp.l2_district_type, icp.district_population
        from {{ ref("int__icp_offices") }} as icp
        join
            substrate_covered
            on substrate_covered.state = icp.state
            and substrate_covered.district_type = icp.l2_district_type
        where
            icp.is_matched
            and not coalesce(icp.is_judicial, false)
            and not coalesce(icp.is_appointed, false)
    )

select
    l2_district_type,
    count(*) as offices,
    count(case when district_population is null then 1 end) as unsized_offices
from sizeable_offices
group by l2_district_type
having
    count(*) >= 100
    and count(case when district_population is null then 1 end) * 1.0 / count(*) > 0.35
