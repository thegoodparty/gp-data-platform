-- Every adopted proposed district must carry top issues; without this the
-- routed onboarding voter-issues endpoint silently returns nothing.
--
-- Scoped to districts whose voters are actually present in the scored source.
-- The two sides refresh on different cadences: the district mart is built from
-- int__l2_nationwide_uniform, while the issue scores come from
-- int__l2_nationwide_uniform_w_haystaq, which is incremental-merge and pinned
-- full_refresh=False. A newly adopted district whose voters have not merged into
-- that table yet would otherwise fail this error-severity test while nothing is
-- wrong. Restricting to districts the scorer has actually seen keeps the test
-- firing on a real gap — a district that IS scored but produced no issue rows —
-- rather than on a refresh lag.
with
    scored_districts as (
        select distinct state_postal_code as state, proposed_district
        from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}
        where proposed_district is not null
    )
select district.state, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
inner join
    scored_districts
    on scored_districts.state = district.state
    and scored_districts.proposed_district = district.l2_district_name
left join
    {{ ref("m_election_api__district_top_issues") }} as issues
    on issues.l2_state = district.state
    and issues.l2_district_type = district.l2_district_type
    and issues.l2_district_name = district.l2_district_name
where district.l2_district_type = 'Proposed_District' and issues.l2_state is null
