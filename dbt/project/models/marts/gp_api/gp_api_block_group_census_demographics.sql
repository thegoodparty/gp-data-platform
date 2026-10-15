select
    geoid as block_group_geoid,
    {{
        dbt_utils.star(
            ref("stg_census_acs__geo_estimates"), except=["summary_level", "geoid"]
        )
    }}
from {{ ref("stg_census_acs__geo_estimates") }}
where summary_level = '150'
