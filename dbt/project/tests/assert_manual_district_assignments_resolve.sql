-- Every manual district assignment must reach m_election_api__district, which is
-- what l2_br_match_overrides joins a BR position to. Editing the assignment seed
-- does not bump voter loaded_at, so int__l2_district_aggregations unions the seed
-- into its incremental candidate set to catch up on the next ordinary run. This
-- fails in the window before that run, rather than leaving the seat silently
-- unselectable.
--
-- The district must also hold voters. The aggregation keeps a district row once
-- it exists, so an assignment that stops matching (L2 respells the parent a
-- sub-district guard compares against, a county renumbers its precincts) leaves
-- an empty district that positions still point at and the picker still lists.
select distinct
    assignments.state, assignments.l2_district_type, assignments.l2_district_name
from {{ ref("l2_manual_district_assignments") }} as assignments
left join
    {{ ref("m_election_api__district") }} as districts
    on districts.state = assignments.state
    and districts.l2_district_type = assignments.l2_district_type
    and districts.l2_district_name = assignments.l2_district_name
where districts.id is null or coalesce(districts.registered_voters, 0) = 0
