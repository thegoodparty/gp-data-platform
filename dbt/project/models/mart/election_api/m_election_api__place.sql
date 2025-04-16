{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "place"],
    )
}}

with
    place_ids_in_races as (
        select distinct place_id from {{ ref("int__enhanced_race") }}
    ),
    parent_ids as (
        select distinct parent_id
        from {{ ref("int__enhanced_place_w_parent") }}
        where id in (select place_id from place_ids_in_races)
    ),
    grandparent_ids as (
        select distinct tbl_parent.parent_id as grandparent_id
        from {{ ref("int__enhanced_place_w_parent") }} as tbl_parent
        where tbl_parent.id in (select parent_id from parent_ids)
    ),
    greatgrandparent_ids as (
        select distinct tbl_parent.parent_id as greatgrandparent_id
        from {{ ref("int__enhanced_place_w_parent") }} as tbl_parent
        where tbl_parent.id in (select grandparent_id from grandparent_ids)
    )
select
    tbl_place.id,
    tbl_place.created_at,
    tbl_place.updated_at,
    tbl_place.br_database_id,
    replace(tbl_place.`name`, 'CCD', '') as name,
    replace(tbl_place.place_name_slug, '-ccd', '') as slug,
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
    tbl_place.parent_id
from {{ ref("int__enhanced_place_w_parent") }} as tbl_place
where
    tbl_place.geo_id is not null
    and tbl_place.place_name_slug is not null
    and tbl_place.name is not null
    and (
        tbl_place.id in (select place_id from place_ids_in_races)
        or tbl_place.id in (select parent_id from parent_ids)
        or tbl_place.id in (select grandparent_id from grandparent_ids)
        or tbl_place.id in (select greatgrandparent_id from greatgrandparent_ids)
    )
    {% if is_incremental() %}
        and tbl_place.updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
