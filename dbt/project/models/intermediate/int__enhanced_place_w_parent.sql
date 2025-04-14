{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_place_w_parent"],
    )
}}


with

    enhanced_place as (
        select
            tbl_place.id,
            tbl_place.created_at,
            tbl_place.updated_at,
            tbl_place.br_database_id,
            tbl_place.name,
            tbl_place.geo_id,
            tbl_place.mtfcc,
            tbl_place.state,
            tbl_place.slug,
            tbl_place.place_name_slug,
            tbl_place.city_largest,
            tbl_place.county_name,
            tbl_place.population,
            tbl_place.density,
            tbl_place.income_household_median,
            tbl_place.unemployment_rate,
            tbl_place.home_value,
            tbl_geo_id.parent_geo_id
        from {{ ref("int__enhanced_place") }} as tbl_place
        left join
            {{ ref("int__geo_id_attributes") }} as tbl_geo_id
            on tbl_place.geo_id = tbl_geo_id.geo_id
            and tbl_place.mtfcc = tbl_geo_id.mtfcc
        {% if is_incremental() %}
            where tbl_place.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    with_parent_id as (
        select distinct
            tbl_place.id,
            tbl_place.created_at,
            tbl_place.updated_at,
            tbl_place.br_database_id,
            tbl_place.name,
            tbl_place.slug,
            tbl_place.place_name_slug,
            tbl_place.geo_id,
            tbl_place.mtfcc,
            tbl_place.state,
            tbl_place.city_largest,
            tbl_place.county_name,
            tbl_place.population,
            tbl_place.density,
            tbl_place.income_household_median,
            tbl_place.unemployment_rate,
            tbl_place.home_value,
            tbl_place_parent.id as parent_id,
            tbl_place_parent.geo_id as parent_geo_id
        from enhanced_place as tbl_place
        left join
            {{ ref("int__enhanced_place") }} as tbl_place_parent
            on tbl_place.parent_geo_id = tbl_place_parent.geo_id

    )

select *
from with_parent_id
