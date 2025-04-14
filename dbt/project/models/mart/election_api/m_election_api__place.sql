{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "place"],
    )
}}


select
    tbl_place.id,
    tbl_place.created_at,
    tbl_place.updated_at,
    tbl_place.br_database_id,
    tbl_place.`name`,
    tbl_place.place_name_slug as slug,
    tbl_place.geo_id as geoid,
    tbl_place.mtfcc,
    tbl_place.`state`,
    tbl_place.city_largest,
    tbl_place.county_name,
    tbl_place.population,
    tbl_place.density,
    tbl_place.income_household_median,
    tbl_place.unemployment_rate,
    tbl_place.home_value,
    tbl_place.parent_geo_id,
    tbl_place.parent_id
from {{ ref("int__enhanced_place_w_parent") }} as tbl_place
where
    tbl_place.geo_id is not null
    and tbl_place.place_name_slug is not null
    and tbl_place.name is not null
    {% if is_incremental() %}
        and tbl_place.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
