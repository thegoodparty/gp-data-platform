-- DATA-1958: every l2_br_match_overrides row must surface in
-- m_election_api__position with the district the override specifies.
--
-- The override seed is the source of truth for hand-corrected and
-- snapshot-absent BR Position <-> L2 District mappings. The position mart
-- applies it via two paths: matched_positions corrects an existing LLM match
-- row, and override_injected_positions injects a match for positions absent
-- from the LLM match snapshot. This test guards both paths. It fails if an
-- override is silently dropped (position missing from the mart, or a null
-- district_id) or not honored (district_id does not equal the override's
-- district).
--
-- Relies on m_election_api__district being unique per
-- (state, l2_district_type, l2_district_name) -- tested separately in
-- m_election_api.yaml -- so the expected district id is unambiguous. The case
-- where an override's district does not exist at all is covered by the seed's
-- own expression_is_true test in seeds_schema.yaml.
--
-- Returns one row per override that is not correctly reflected.
with
    override_expected as (
        select o.br_database_id, d.id as expected_district_id
        from {{ ref("l2_br_match_overrides") }} as o
        inner join
            {{ ref("m_election_api__district") }} as d
            on o.state = d.state
            and o.l2_district_type = d.l2_district_type
            and o.l2_district_name = d.l2_district_name
    )
select e.br_database_id, e.expected_district_id, p.district_id as actual_district_id
from override_expected as e
left join
    {{ ref("m_election_api__position") }} as p on e.br_database_id = p.br_database_id
where
    p.br_database_id is null
    or p.district_id is null
    or p.district_id != e.expected_district_id
