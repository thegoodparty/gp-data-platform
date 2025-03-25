{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        tags=["intermediate", "ballotready", "position_fun_facts"],
    )
}}

with
    deduped_cities as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by county_fips order by population desc
                    ) as rn
                from {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }}
            ) ranked
        where rn = 1
    ),
    joined_by_geo_id as (
        select
            tbl_position.database_id,
            tbl_position.geo_id,
            tbl_position.name,
            tbl_position.slug,
            tbl_position.state,
            tbl_position.updated_at,
            tbl_cities.county_name,
            tbl_cities.county_fips,
            tbl_cities.city,
            tbl_cities.zips,
            tbl_cities.csa_name,
            tbl_cities.population,
            tbl_cities.density,
            tbl_cities.home_value,
            tbl_cities.unemployment_rate,
            tbl_cities.income_household_median
        from {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_position
        left join
            deduped_cities as tbl_cities
            on substring(tbl_position.geo_id, 1, 5) = tbl_cities.county_fips
            and tbl_position.state = tbl_cities.state_id
        {% if is_incremental() %}
            where tbl_position.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    unmatched as (select * from joined_by_geo_id where county_fips is null),
    string_matched_city_state as (
        select
            tbl_unmatched.database_id,
            tbl_unmatched.geo_id,
            tbl_unmatched.name,
            tbl_unmatched.slug,
            tbl_unmatched.state,
            tbl_unmatched.updated_at,
            coalesce(tbl_unmatched.county_name, tbl_cities.county_name) as county_name,
            coalesce(tbl_unmatched.county_fips, tbl_cities.county_fips) as county_fips,
            coalesce(tbl_unmatched.city, tbl_cities.city) as city,
            coalesce(tbl_unmatched.zips, tbl_cities.zips) as zips,
            coalesce(tbl_unmatched.csa_name, tbl_cities.csa_name) as csa_name,
            coalesce(tbl_unmatched.population, tbl_cities.population) as population,
            coalesce(tbl_unmatched.density, tbl_cities.density) as density,
            coalesce(tbl_unmatched.home_value, tbl_cities.home_value) as home_value,
            coalesce(
                tbl_unmatched.unemployment_rate, tbl_cities.unemployment_rate
            ) as unemployment_rate,
            coalesce(
                tbl_unmatched.income_household_median,
                tbl_cities.income_household_median
            ) as income_household_median
        from unmatched as tbl_unmatched
        left join
            {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }} as tbl_cities
            on tbl_unmatched.name ilike concat('%', tbl_cities.city, '%')
            and tbl_unmatched.state = tbl_cities.state_id
    ),
    deduped_matched_by_string as (
        select
            database_id,
            geo_id,
            name,
            slug,
            state,
            updated_at,
            county_name,
            county_fips,
            city,
            zips,
            csa_name,
            population,
            density,
            home_value,
            unemployment_rate,
            income_household_median
        from
            (
                select
                    *,
                    row_number() over (
                        partition by database_id order by population desc
                    ) as rn
                from string_matched_city_state
            ) ranked
        where rn = 1
    ),
    final as (
        select *
        from joined_by_geo_id
        where county_fips is not null
        union all
        select *
        from deduped_matched_by_string
    )

select
    database_id,
    geo_id,
    name,
    slug,
    state,
    updated_at,
    county_name,
    county_fips,
    city,
    zips,
    csa_name,
    population,
    density,
    home_value,
    unemployment_rate,
    income_household_median
from final
