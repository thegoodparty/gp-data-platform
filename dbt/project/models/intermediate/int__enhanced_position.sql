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
            tbl_position.database_id as br_database_id,
            tbl_position.id as br_position_id,
            tbl_position.`name`,
            tbl_position.geo_id,
            tbl_position.mtfcc,
            tbl_position.`state`,
            tbl_position.slug,
            tbl_fast_facts.city as city_largest,
            tbl_fast_facts.county_name as county_name,
            try_cast(tbl_fast_facts.population as int) as population,
            try_cast(tbl_fast_facts.density as float) as density,
            try_cast(
                tbl_fast_facts.income_household_median as int
            ) as income_household_median,
            try_cast(tbl_fast_facts.unemployment_rate as float) as unemployment_rate,
            try_cast(tbl_fast_facts.home_value as int) as home_value
        -- parent_id is self-referential, it is added in an additional layer
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
        left join
            {{ ref("int__position_fast_facts") }} as tbl_fast_facts
            on tbl_position.database_id = tbl_fast_facts.database_id
        {% if is_incremental() %}
            where tbl_position.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    with_slugged_location as (
        select
            id,
            created_at,
            updated_at,
            br_database_id,
            br_position_id,
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
            home_value
        from enhanced_position
    )

select
    id,
    created_at,
    updated_at,
    br_database_id,
    br_position_id,
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
    home_value
from with_slugged_location
