{{ config(severity="error") }}

-- Independent pure-relabel anchor for the Connecticut mapping: the 2022
-- recode changed only the county segment, so per row the state segment
-- (characters 1-2) and the tract+block suffix (characters 6-15) must be
-- identical across the two code systems, checked on the RAW values. If this
-- ever fails, the file is not a pure relabel and the coverage-allowlist
-- decision must be revisited before anything consumes the mapping.
select block_fips_2020, block_fips_2022
from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
where
    substring(block_fips_2020, 1, 2) != substring(block_fips_2022, 1, 2)
    or substring(block_fips_2020, 6) != substring(block_fips_2022, 6)
