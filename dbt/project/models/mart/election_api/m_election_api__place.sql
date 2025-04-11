{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "place"],
    )
}}


with
    deduped_place as (
        select *
        from {{ ref("int__enhanced_place") }}
        qualify row_number() over (partition by geo_id order by population desc) = 1
    )
select
    tbl_pos.id,
    tbl_pos.created_at,
    tbl_pos.updated_at,
    tbl_pos.br_database_id as br_position_database_id,
    tbl_place.`name`,
    tbl_pos.slug,
    tbl_pos.geo_id as geoid,
    tbl_pos.mtfcc,
    tbl_pos.`state`,
    tbl_pos.city_largest,
    tbl_pos.county_name,
    tbl_pos.population,
    tbl_pos.density,
    tbl_pos.income_household_median,
    tbl_pos.unemployment_rate,
    tbl_pos.home_value,
    tbl_pos.parent_id
from {{ ref("int__enhanced_position_w_parent") }} as tbl_pos  -- note that the position table is used for the election place table
left join deduped_place as tbl_place on tbl_pos.geo_id = tbl_place.geo_id
where
    tbl_pos.geo_id is not null and tbl_pos.slug is not null
    {% if is_incremental() %}
        and tbl_pos.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
