-- Every manual district assignment must reach m_election_api__district, which is
-- what l2_br_match_overrides joins a BR position to. Editing the assignment seed
-- does not bump voter loaded_at, so int__l2_district_aggregations unions the seed
-- into its incremental candidate set to catch up on the next ordinary run. This
-- fails in the window before that run, rather than leaving the seat silently
-- unselectable.
select distinct
    assignments.state, assignments.l2_district_type, assignments.l2_district_name
from {{ ref("l2_manual_district_assignments") }} as assignments
left join
    {{ ref("m_election_api__district") }} as districts
    on districts.state = assignments.state
    and districts.l2_district_type = assignments.l2_district_type
    and districts.l2_district_name = assignments.l2_district_name
where districts.id is null
