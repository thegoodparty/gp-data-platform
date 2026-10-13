-- Block-group ACS counts for gp-api, keyed by the 12-digit block group geoid that
-- gp_api_voters carries per person. Serve weights the block groups its outreach
-- respondents live in against the district's composition in
-- gp_api_district_census_stats, so both sides read the same census source.
select geoid as block_group_geoid, * except (summary_level, geoid)
from {{ ref("int__census_acs_demographics") }}
where summary_level = '150'
