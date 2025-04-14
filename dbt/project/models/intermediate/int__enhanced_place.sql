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
            tbl_place.database_id as br_database_id,
            tbl_place.`name`,
            {{ slugify("tbl_place.`name`") }} as slugified_name,
            tbl_place.geo_id,
            tbl_place.mtfcc,
            tbl_place.`state`,
            tbl_place.slug,
            tbl_fun_facts.city as city_largest,
            tbl_fun_facts.county_name as county_name,
            tbl_fun_facts.population as population,
            tbl_fun_facts.density as density,
            tbl_fun_facts.income_household_median as income_household_median,
            tbl_fun_facts.unemployment_rate as unemployment_rate,
            tbl_fun_facts.home_value as home_value,
            tbl_geo_id.place_name_slug,
            tbl_geo_id.parent_geo_id
        -- parent_id is self-referential, it is added in an additional layer
        from {{ ref("stg_airbyte_source__ballotready_api_place") }} as tbl_place
        left join
            {{ ref("int__place_fun_facts") }} as tbl_fun_facts
            on tbl_place.database_id = tbl_fun_facts.database_id
        left join
            {{ ref("int__geo_id_attributes") }} as tbl_geo_id
            on tbl_place.geo_id = tbl_geo_id.geo_id
        {% if is_incremental() %}
            where tbl_place.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
        -- in 254/77k cases, the geo_id is not unique, so we need to deduplicate by
        -- taking the short slug
        -- which filters out specific areas like schools and fire districts
        qualify
            row_number() over (
                partition by tbl_place.geo_id order by tbl_place.slug asc
            )
            = 1
    )

select
    id,
    created_at,
    updated_at,
    br_database_id,
    `name`,
    slugified_name,
    geo_id,
    mtfcc,
    `state`,
    slug,
    city_largest,
    county_name,
    population,
    density,
    income_household_median,
    unemployment_rate,
    home_value,
    place_name_slug,
    parent_geo_id
from enhanced_place
