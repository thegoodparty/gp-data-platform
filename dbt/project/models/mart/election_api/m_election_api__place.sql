{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "place"],
    )
}}


select
    id,
    created_at,
    updated_at,
    br_database_id as br_position_database_id,
    `name`,
    slug,
    geo_id,
    mtfcc,
    `state`,
    city_largest,
    county_name,
    population,
    density,
    income_household_median,
    unemployment_rate,
    home_value
from {{ ref("int__enhanced_position") }}  -- note that the position table is used for the election place table
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
