-- Every adopted proposed district must carry top issues; without this the
-- routed onboarding voter-issues endpoint silently returns nothing.
--
-- Scoped to states the scorer has already reached. The two sides refresh on
-- different cadences: the district mart is built from int__l2_nationwide_uniform,
-- while the issue scores come from int__l2_nationwide_uniform_w_haystaq, which is
-- incremental-merge and pinned full_refresh=False. A state whose voters have not
-- merged into that table yet would otherwise fail this error-severity test while
-- nothing is wrong.
--
-- That scope is read off the top-issues mart rather than the haystaq table it is
-- built from, because that table is tagged `monthly`. CI runs with
-- --exclude tag:monthly, and dbt's eager indirect selection drops any test whose
-- parents include an excluded node — referencing it directly made this guard
-- inert in CI, which is the failure mode the guard exists to prevent.
with
    scored_states as (
        select distinct l2_state
        from {{ ref("m_election_api__district_top_issues") }}
        where l2_district_type = 'Proposed_District'
    )
select district.state, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
inner join scored_states on scored_states.l2_state = district.state
left join
    {{ ref("m_election_api__district_top_issues") }} as issues
    on issues.l2_state = district.state
    and issues.l2_district_type = district.l2_district_type
    and issues.l2_district_name = district.l2_district_name
where district.l2_district_type = 'Proposed_District' and issues.l2_state is null
