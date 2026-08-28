-- Every l2_br_match_overrides row must surface in
-- m_election_api__position with the district the override specifies. Fails if
-- an override is dropped (missing position or null district) or not honored
-- (wrong district). Combo uniqueness and existence are tested separately in
-- m_election_api.yaml and seeds_schema.yaml.
with
    -- Same spelling resolution the position model applies, so an override that
    -- names a stale L2 spelling is expected to land on the populated sibling.
    resolved_districts as ({{ l2_district_spelling_resolution() }}),
    override_expected as (
        select o.br_database_id, d.district_id as expected_district_id
        from {{ ref("l2_br_match_overrides") }} as o
        inner join
            resolved_districts as d
            on o.state = d.state
            and o.l2_district_type = d.l2_district_type
            and o.l2_district_name = d.l2_district_name
    )
select e.br_database_id, e.expected_district_id, p.district_id as actual_district_id
from override_expected as e
left join
    {{ ref("m_election_api__position") }} as p
    on e.br_database_id = cast(p.br_database_id as bigint)
where
    p.br_database_id is null
    or p.district_id is null
    or p.district_id != e.expected_district_id
