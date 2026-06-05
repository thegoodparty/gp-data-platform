{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="database_id",
        tags=["intermediate", "ballotready", "place_fast_facts"],
    )
}}

with
    -- per-county dedup; used only by the non-G4110 substring path below
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

    -- Per-(state, normalized name) dedup for the G4110 exact join: one row per key
    -- keeps the join 1:1 (protects equal_rowcount) and resolves same-name collisions
    -- to the most-populous match, deterministically.
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

    -- One row per (state, normalized county name) for the G5420 county match. Two
    -- correctness rules beyond a plain dedup (see DATA-1950 PR #443 Codex review):
    -- 1. order by county_fips ASC (not population): when a county shares its name with
    -- an independent city in the same state (Baltimore, St. Louis MO, Richmond /
    -- Roanoke / Franklin VA), US FIPS always numbers the independent city higher
    -- (510+), so the LOWER county_fips is the actual county. Population-desc would
    -- wrongly pick the larger independent city (e.g. Baltimore County PS -> Baltimore
    -- city). The lower-FIPS county is correct.
    -- 2. where population is not null: drops degenerate cross-state stray rows (e.g. a
    -- state_id='GA' row carrying TN county_fips 47139) that create false ambiguity and
    -- could otherwise win county_fips-asc from the wrong state (e.g. OH Allen vs IN).
    -- Keeps the (state, county_name) join key 1:1 (protects equal_rowcount).
    deduped_counties_by_name as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by state_id, lower(county_name)
                        order by county_fips asc, population desc nulls last
                    ) as rn
                from {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }}
                where population is not null
            ) ranked
        where rn = 1
    ),

    -- mtfcc is read only to split the branches below; it is not in the final output
    places as (
        select database_id, geo_id, name, slug, state, updated_at, mtfcc
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    ),

    -- Non-G4110 branch: byte-identical to the pre-fix model (only the mtfcc filter is
    -- added) so the same-snapshot non-G4110 diff stays 0. A null mtfcc routes here too
    -- (the legacy path); assert_place_mtfcc_present fails loudly if one appears, since
    -- mtfcc is the only branch signal and is non-null for all places today.
    non_g4110_places as (
        select * from places where (mtfcc not in ('G4110', 'G5420')) or mtfcc is null
    ),
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

    -- G4110 resolution chain: exact name+state match -> ILIKE fallback -> NULL
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
        -- NOTE: normalized_name (g4110_places) and normalized_city
        -- (deduped_cities_by_name) MUST use the identical normalization
        -- expression. If you edit one, edit BOTH, or this exact-match join
        -- silently returns zero matches and every G4110 place falls through
        -- to the ILIKE/NULL path.
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

    -- G5420 (school district) branch (spec D2/D3): strip the district-type suffix to
    -- a candidate
    -- place name; if it ends in " county" -> county match (or NULL, no city
    -- fall-through);
    -- otherwise city primary -> city retry (strip one trailing place-type word) ->
    -- NULL. The
    -- strip peels only trailing DESCRIPTOR words + district numbers/class codes and
    -- KEEPS place
    -- nouns so "Dodge City"/"Mount Union" survive. n0 reuses the SAME base
    -- normalization as
    -- g4110_places / deduped_cities_by_name — keep in sync. No same-select lateral
    -- aliases
    -- (n0 is a g5420_base column; city_cand a g5420_candidates column), so it
    -- compiles broadly.
    g5420_base as (
        select
            database_id,
            geo_id,
            name,
            slug,
            state,
            updated_at,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(name, ' *\\([^)]*\\) *$', ''), ' +', ' '
                    )
                )
            ) as n0
        from places
        where mtfcc = 'G5420'
    ),
    g5420_candidates as (
        select
            database_id,
            geo_id,
            name,
            slug,
            state,
            updated_at,
            n0,
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            n0,
                            ' ((no[.]?|number|#) ?)?([0-9][0-9a-z-]*|r-[ivxlc]+|re-?[0-9]+[a-z]?|[a-z]-[0-9]+[a-z]?) *$',
                            ''
                        ),
                        ' ((union free|community consolidated|community unit|community|consolidated|independent|unified|central|area|regional|elementary|high|joint|cooperative|special|graded|common|metropolitan|public) )?(school district|school districts|school corporation|public schools|schools|school|district) *$',
                        ''
                    ),
                    ' (r-[ivxlc]+|re-?[0-9]+[a-z]?|[a-z]-[0-9]+[a-z]?|[0-9]+) *$',
                    ''
                )
            ) as city_cand
        from g5420_base
    ),
    g5420_candidates2 as (
        select
            database_id,
            geo_id,
            name,
            slug,
            state,
            updated_at,
            city_cand,
            case
                when city_cand rlike ' county$'
                then trim(regexp_replace(city_cand, ' county$', ''))
            end as county_cand,
            trim(
                regexp_replace(
                    city_cand, ' (city|town|township|twp|borough|boro|village) *$', ''
                )
            ) as city_cand_retry
        from g5420_candidates
    ),
    g5420_resolved as (
        select
            c.database_id,
            c.geo_id,
            c.name,
            c.slug,
            c.state,
            c.updated_at,
            coalesce(
                case when c.county_cand is not null then cnty.county_name end,
                cp.county_name,
                crt.county_name
            ) as county_name,
            coalesce(
                case when c.county_cand is not null then cnty.county_fips end,
                cp.county_fips,
                crt.county_fips
            ) as county_fips,
            coalesce(
                case when c.county_cand is not null then cnty.city end,
                cp.city,
                crt.city
            ) as city,
            coalesce(
                case when c.county_cand is not null then cnty.zips end,
                cp.zips,
                crt.zips
            ) as zips,
            coalesce(
                case when c.county_cand is not null then cnty.csa_name end,
                cp.csa_name,
                crt.csa_name
            ) as csa_name,
            coalesce(
                case when c.county_cand is not null then cnty.population end,
                cp.population,
                crt.population
            ) as population,
            coalesce(
                case when c.county_cand is not null then cnty.density end,
                cp.density,
                crt.density
            ) as density,
            coalesce(
                case when c.county_cand is not null then cnty.home_value end,
                cp.home_value,
                crt.home_value
            ) as home_value,
            coalesce(
                case when c.county_cand is not null then cnty.unemployment_rate end,
                cp.unemployment_rate,
                crt.unemployment_rate
            ) as unemployment_rate,
            coalesce(
                case
                    when c.county_cand is not null then cnty.income_household_median
                end,
                cp.income_household_median,
                crt.income_household_median
            ) as income_household_median
        from g5420_candidates2 as c
        left join
            deduped_counties_by_name as cnty
            on c.county_cand is not null
            and cnty.state_id = c.state
            and lower(cnty.county_name) = c.county_cand
            and length(c.county_cand) >= 2
        left join
            deduped_cities_by_name as cp
            on c.county_cand is null
            and cp.state_id = c.state
            and cp.normalized_city = c.city_cand
            and length(c.city_cand) >= 3
        left join
            deduped_cities_by_name as crt
            on c.county_cand is null
            and crt.state_id = c.state
            and crt.normalized_city = c.city_cand_retry
            and crt.normalized_city <> c.city_cand
            and length(c.city_cand_retry) >= 3
    ),

    final as (
        select *
        from non_g4110_resolved
        union all
        select *
        from g4110_resolved
        union all
        select *
        from g5420_resolved
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
