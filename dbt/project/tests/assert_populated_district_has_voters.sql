-- A district that L2 says holds voters must have DistrictVoter rows.
--
-- The existing tests only check the other direction: that a DistrictVoter row
-- points at a real district and a real voter. Nothing caught a district losing
-- its whole voter set, which is how ~2,500 districts reached serving reporting
-- zero constituents. The exact-match join returns nothing rather than erroring,
-- so the failure stayed silent all the way to the customer.
--
-- This only holds once the Voter and DistrictVoter marts have rebuilt with the
-- seven district columns the Voter mart was missing. Against a DistrictVoter
-- built before that, it reports exactly those types' districts.
--
-- Scoped to the district types currently collected from L2. 'State' is excluded
-- because DistrictStats derives statewide associations straight from
-- Voter.state, so those synthetic rows carry no DistrictVoter links. Types
-- absent from the list are excluded because the aggregation is append-only and
-- keeps rows for types since dropped from it; those have no voter column left to
-- link through, and pruning them is a separate job.
select district.id, district.state, district.l2_district_type, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
left join
    (
        select distinct district_id from {{ ref("m_people_api__districtvoter") }}
    ) as linked
    on district.id = linked.district_id
where
    district.registered_voters > 0
    and linked.district_id is null
    and district.l2_district_type in (
        {%- for district_type in get_l2_district_types() %}
            '{{ district_type }}'{{ "," if not loop.last }}
        {%- endfor %}
    )
