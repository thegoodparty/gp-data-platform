-- A (zip, office) pair must appear exactly once. m_election_api__zip_to_position
-- sums voters_in_zip_district per (zip, office), so a second row for the same
-- pair -- e.g. one office joining two spellings of one district -- silently
-- double-counts that district's voters in pct_districtzip_to_zip.
select zip_code, br_database_id, count(*) as n_rows
from {{ ref("int__zip_code_to_br_office") }}
group by zip_code, br_database_id
having count(*) > 1
