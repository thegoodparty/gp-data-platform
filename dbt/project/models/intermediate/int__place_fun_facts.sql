{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "place_fun_facts"],
    )
}}

with
    joined_by_geo_id as (
        select
            cast(tbl_place.databaseid as int) as database_id,
            tbl_place.geoid,
            tbl_place.name,
            tbl_place.slug,
            tbl_place.state,
            tbl_cities.county_name as county_name,
            tbl_cities.county_fips as county_fips,
            tbl_cities.city,
            tbl_cities.zips,
            tbl_cities.csa_name
        from {{ ref("stg_airbyte_source__ballotready_api_place") }} as tbl_place
        left join
            {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }} as tbl_cities
            on substring(tbl_place.geoid, 1, 5) = tbl_cities.county_fips
    ),

/*
with

    enhanced_place as (
        select
            {{ generate_salted_uuid(fields=["id"], salt="ballotready") }} as id,
            created_at,
            updated_at,
            -- id as br_hash_id, # should be added to API data model
            database_id as br_database_id,
            `name`,
            slug,
            geo_id,
            mtfcc,
            `state`
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select id, created_at, updated_at, br_database_id, `name`, slug, geo_id, mtfcc, `state`
from enhanced_place
*/
select *
from joined_by_geo_id
