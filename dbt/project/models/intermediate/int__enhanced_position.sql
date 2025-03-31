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
            {{ generate_salted_uuid(fields=["tbl_position.id"], salt="ballotready") }}
            as id,
            tbl_position.created_at,
            tbl_position.updated_at,
            -- id as br_hash_id, # should be added to API data model
            tbl_position.database_id as br_database_id,
            tbl_position.`name`,
            tbl_position.slug,
            tbl_position.geo_id,
            tbl_position.mtfcc,
            tbl_position.`state`,
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
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
        left join
            {{ ref("int__position_fun_facts") }} as tbl_fun_facts
            on tbl_position.database_id = tbl_fun_facts.database_id
        {% if is_incremental() %}
            where tbl_position.updated_at > (select max(updated_at) from {{ this }})
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
from enhanced_position
