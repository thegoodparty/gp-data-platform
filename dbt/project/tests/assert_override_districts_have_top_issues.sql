-- The top-issues mart scores override districts as well as matched ones, so a
-- position served through the override seed alone (a minted district, or one
-- the matcher abstained on) still gets voter issues. Every override district
-- that L2 populates must appear; Haystaq scores every state, so an absence is
-- the override leg or its spelling resolution failing, not missing scores.
with
    resolved_districts as ({{ l2_district_spelling_resolution() }}),
    override_districts as (
        select distinct
            tbl_district.state,
            tbl_district.l2_district_type,
            tbl_district.l2_district_name
        from {{ ref("l2_br_match_overrides") }} as tbl_override
        inner join
            resolved_districts as tbl_resolved
            on tbl_override.state = tbl_resolved.state
            and tbl_override.l2_district_type = tbl_resolved.l2_district_type
            and tbl_override.l2_district_name = tbl_resolved.l2_district_name
        inner join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_resolved.district_id = tbl_district.id
        where tbl_district.registered_voters > 0
    )
select
    override_districts.state,
    override_districts.l2_district_type,
    override_districts.l2_district_name
from override_districts
left join
    {{ ref("m_election_api__district_top_issues") }} as tbl_issues
    on tbl_issues.l2_state = override_districts.state
    and tbl_issues.l2_district_type = override_districts.l2_district_type
    and tbl_issues.l2_district_name = override_districts.l2_district_name
where tbl_issues.l2_state is null
