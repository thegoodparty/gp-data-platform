-- voters_in_zip must be invariant per (zip_code, br_database_id).
--
-- m_election_api__zip_to_position uses any_value(voters_in_zip) when
-- aggregating int__zip_code_to_br_office to (zip_code, br_database_id) grain.
-- That is only deterministic if voters_in_zip is constant within each group.
-- If a single br_database_id ever resolves from rows with different
-- district_types (e.g. via the LLM match), voters_in_zip would diverge across
-- the input rows and any_value() would silently pick a non-deterministic one.
--
-- Each row this test returns is a (zip_code, br_database_id) pair that
-- violates the invariant.
select zip_code, br_database_id, count(distinct voters_in_zip) as n_distinct
from {{ ref("int__zip_code_to_br_office") }}
where br_database_id is not null
group by zip_code, br_database_id
having count(distinct voters_in_zip) > 1
