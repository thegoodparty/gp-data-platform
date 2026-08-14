{{ config(severity="error") }}

-- Every adopted proposed district must carry top issues; without this the
-- routed onboarding voter-issues endpoint silently returns nothing.
select d.state, d.l2_district_name
from {{ ref("m_election_api__district") }} as d
left join
    {{ ref("m_election_api__district_top_issues") }} as t
    on t.l2_state = d.state
    and t.l2_district_type = d.l2_district_type
    and t.l2_district_name = d.l2_district_name
where d.l2_district_type = 'Proposed_District' and t.l2_district_name is null
