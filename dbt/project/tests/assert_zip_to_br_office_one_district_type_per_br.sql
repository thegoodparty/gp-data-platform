-- Each (zip_code, br_database_id) must resolve from exactly one district_type.
--
-- The mart m_election_api__zip_to_position aggregates int__zip_code_to_br_office
-- by (zip_code, br_database_id) using sum(voters_in_zip_district) as the
-- numerator and any_value(voters_in_zip) as the denominator. Both are correct
-- only if every br_database_id within a zip is sourced from a single
-- district_type — voters_in_zip is invariant per (zip, district_type), not per
-- (zip, br_database_id), so multiple district_types contributing to the same
-- br_database_id would let the numerator sum across populations while the
-- denominator picks just one of them, producing pct_districtzip_to_zip > 1.0.
--
-- The voters_in_zip-divergence canary catches most of this case implicitly,
-- but two district_types with coincidentally equal voters_in_zip would slip
-- through that test while still allowing the ratio to exceed 1.0. This test
-- closes that gap by asserting the structural invariant directly.
--
-- Each row this test returns is a (zip_code, br_database_id) pair sourced
-- from more than one district_type.
select zip_code, br_database_id, count(distinct district_type) as n_district_types
from {{ ref("int__zip_code_to_br_office") }}
where br_database_id is not null
group by zip_code, br_database_id
having count(distinct district_type) > 1
