{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["intermediate", "ballotready", "enhanced_place"],
    )
}}


with

    enhanced_place as (
        select
            {{ generate_salted_uuid(fields=["tbl_place.id"], salt="ballotready") }}
            as id,
            tbl_place.created_at,
            tbl_place.updated_at,
            -- id as br_hash_id, # should be added to API data model
            tbl_place.database_id as br_database_id,
            tbl_place.`name`,
            tbl_place.slug,
            tbl_place.geo_id,
            tbl_place.mtfcc,
            tbl_place.`state`,
            tbl_fun_facts.city as city_largest,
            tbl_fun_facts.county_name as county_name,
            tbl_fun_facts.population as population,
            tbl_fun_facts.density as density,
            tbl_fun_facts.income_household_median as income_household_median,
            tbl_fun_facts.unemployment_rate as unemployment_rate,
            tbl_fun_facts.home_value as home_value
        /* TODO: add the following
                // Relations
                Race     Race[]
                // Recursive relationships
                children Place[] @relation("PlaceHierarchy")
                parent   Place?  @relation("PlaceHierarchy", fields: [parentId], references: [id])
                parentId String? @db.Uuid
        */
        from {{ ref("stg_airbyte_source__ballotready_api_place") }} as tbl_place
        left join
            {{ ref("int__place_fun_facts") }} as tbl_fun_facts
            on tbl_place.database_id = tbl_fun_facts.database_id
        {% if is_incremental() %}
            where tbl_place.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select
    id,
    created_at,
    updated_at,
    br_database_id,
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
from enhanced_place
