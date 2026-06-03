{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        tags=["intermediate", "ballotready", "place_fast_facts"],
    )
}}

with
    -- uscities deduped to most-populous city per county.
    -- UNCHANGED: used only by the non-G4110 substring path.
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

    -- NEW: uscities deduped to most-populous city per (state, normalized city_ascii).
    -- Used only by the G4110 exact path. Per-(state,name) dedup => the G4110 join is
    -- 1:1
    -- (protects equal_rowcount) and resolves same-name collisions deterministically.
    deduped_cities_by_name as (
        select * except (rn)
        from
            (
                select
                    *,
                    lower(
                        trim(
                            regexp_replace(
                                regexp_replace(city_ascii, ' *\\([^)]*\\) *$', ''),
                                ' +',
                                ' '
                            )
                        )
                    ) as normalized_city,
                    row_number() over (
                        partition by
                            state_id,
                            lower(
                                trim(
                                    regexp_replace(
                                        regexp_replace(
                                            city_ascii, ' *\\([^)]*\\) *$', ''
                                        ),
                                        ' +',
                                        ' '
                                    )
                                )
                            )
                        order by population desc nulls last, county_fips asc
                    ) as rn
                from {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }}
            ) ranked
        where rn = 1
    ),

    -- place rows, carrying mtfcc internally (not emitted in final)
    places as (
        select database_id, geo_id, name, slug, state, updated_at, mtfcc
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),

    -- --------------------------------------------------------------
    -- NON-G4110 BRANCH: existing logic, filtered to non-G4110.
    -- Unchanged by construction.
    -- --------------------------------------------------------------
    non_g4110_places as (select * from places where mtfcc <> 'G4110' or mtfcc is null),
    joined_by_geo_id as (
        select
            p.database_id,
            p.geo_id,
            p.name,
            p.slug,
            p.state,
            p.updated_at,
            c.county_name,
            c.county_fips,
            c.city,
            c.zips,
            c.csa_name,
            c.population,
            c.density,
            c.home_value,
            c.unemployment_rate,
            c.income_household_median
        from non_g4110_places as p
        left join
            deduped_cities as c
            on substring(p.geo_id, 1, 5) = c.county_fips
            and p.state = c.state_id
    ),
    non_g4110_unmatched as (select * from joined_by_geo_id where county_fips is null),
    non_g4110_string_matched as (
        select
            u.database_id,
            u.geo_id,
            u.name,
            u.slug,
            u.state,
            u.updated_at,
            coalesce(u.county_name, c.county_name) as county_name,
            coalesce(u.county_fips, c.county_fips) as county_fips,
            coalesce(u.city, c.city) as city,
            coalesce(u.zips, c.zips) as zips,
            coalesce(u.csa_name, c.csa_name) as csa_name,
            coalesce(u.population, c.population) as population,
            coalesce(u.density, c.density) as density,
            coalesce(u.home_value, c.home_value) as home_value,
            coalesce(u.unemployment_rate, c.unemployment_rate) as unemployment_rate,
            coalesce(
                u.income_household_median, c.income_household_median
            ) as income_household_median
        from non_g4110_unmatched as u
        left join
            {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }} as c
            on u.name ilike concat('%', c.city, '%')
            and u.state = c.state_id
    ),
    non_g4110_string_deduped as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by database_id order by population desc
                    ) as rn
                from non_g4110_string_matched
            ) ranked
        where rn = 1
    ),
    non_g4110_resolved as (
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
        from joined_by_geo_id
        where county_fips is not null
        union all
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
        from non_g4110_string_deduped
    ),

    -- --------------------------------------------------------------
    -- G4110 BRANCH: exact name+state match -> ILIKE fallback -> NULL
    -- --------------------------------------------------------------
    g4110_places as (
        select
            *,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(name, ' *\\([^)]*\\) *$', ''), ' +', ' '
                    )
                )
            ) as normalized_name
        from places
        where mtfcc = 'G4110'
    ),
    g4110_exact as (
        select
            p.database_id,
            p.geo_id,
            p.name,
            p.slug,
            p.state,
            p.updated_at,
            c.county_name,
            c.county_fips,
            c.city,
            c.zips,
            c.csa_name,
            c.population,
            c.density,
            c.home_value,
            c.unemployment_rate,
            c.income_household_median
        from g4110_places as p
        left join
            deduped_cities_by_name as c
            on p.state = c.state_id
            and p.normalized_name = c.normalized_city
    ),
    g4110_exact_hit as (select * from g4110_exact where county_fips is not null),
    g4110_exact_miss as (
        select database_id, geo_id, name, slug, state, updated_at
        from g4110_exact
        where county_fips is null
    ),
    -- ILIKE fallback: same relation + order as the non-G4110 pass-2, so these rows
    -- are bit-for-bit the current fallback result (no regression).
    g4110_ilike as (
        select
            m.database_id,
            m.geo_id,
            m.name,
            m.slug,
            m.state,
            m.updated_at,
            c.county_name,
            c.county_fips,
            c.city,
            c.zips,
            c.csa_name,
            c.population,
            c.density,
            c.home_value,
            c.unemployment_rate,
            c.income_household_median
        from g4110_exact_miss as m
        left join
            {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }} as c
            on m.name ilike concat('%', c.city, '%')
            and m.state = c.state_id
    ),
    g4110_ilike_deduped as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by database_id order by population desc nulls last
                    ) as rn
                from g4110_ilike
            ) ranked
        where rn = 1
    ),
    g4110_resolved as (
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
        from g4110_exact_hit
        union all
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
        from g4110_ilike_deduped
    ),

    final as (
        select *
        from non_g4110_resolved
        union all
        select *
        from g4110_resolved
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
