{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_position"],
    )
}}


with

    enhanced_position as (
        select
            tbl_pos.id,
            tbl_pos.created_at,
            tbl_pos.updated_at,
            tbl_pos.br_database_id,
            tbl_pos.name,
            tbl_pos.slug,
            tbl_pos.geo_id,
            tbl_pos.mtfcc,
            tbl_pos.state,
            tbl_pos.city_largest,
            tbl_pos.county_name,
            tbl_pos.population,
            tbl_pos.density,
            tbl_pos.income_household_median,
            tbl_pos.unemployment_rate,
            tbl_pos.home_value,
            tbl_pos_parent.id as parent_id
        from {{ ref("int__enhanced_position") }} as tbl_pos
        left join
            {{ ref("int__geo_id_attributes") }} as tbl_geo_id
            on tbl_pos.geo_id = tbl_geo_id.geo_id
        left join
            {{ ref("int__enhanced_position") }} as tbl_pos_parent
            on tbl_geo_id.parent_geo_id = tbl_pos_parent.geo_id
        {% if is_incremental() %}
            where tbl_pos.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select
    id,
    created_at,
    updated_at,
    br_database_id,
    name,
    slug,
    geo_id,
    mtfcc,
    state,
    city_largest,
    county_name,
    population,
    density,
    income_household_median,
    unemployment_rate,
    home_value,
    parent_id
from enhanced_position
