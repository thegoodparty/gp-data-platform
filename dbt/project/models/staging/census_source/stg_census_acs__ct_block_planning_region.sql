-- One row per 2020 Connecticut census block with the ACS 2024 block group it falls in.
-- The ACS keys Connecticut by planning region while the decennial frame and most L2
-- geocodes still carry legacy counties, so every block-to-ACS join goes through this.
select
    block_fips_2020 as block_geoid, left(block_fips_2022, 12) as acs_block_group_geoid
from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
