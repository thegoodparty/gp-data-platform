select block_fips_2020 as block_geoid_2020, block_fips_2022 as block_geoid_2022
from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
