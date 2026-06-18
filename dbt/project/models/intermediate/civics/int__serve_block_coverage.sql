{{ config(materialized="table", tags=["intermediate", "civics", "serve"]) }}

-- int__serve_block_coverage (DATA-1993): the count-once engine. One row per
-- (census block, served_set) carrying the count of DISTINCT served voters and the
-- block's total voters. people_served reads it: count-once for a set =
-- sum(served_voters / total_voters * block_population) over served blocks.
--
-- WHY a voter scan (and not the substrate): count-once is a distinct-PERSON union
-- across overlapping district types. Within one type the cohort districts partition a
-- block's voters, but a voter served by BOTH their city and their county cannot be
-- de-duplicated across types from counts alone -- that cross-type overlap is exactly
-- the "represented by 2+ officials" gap. So the union needs voter IDs, which the
-- counts-only substrate (and int__l2_block_district_map) dropped at the
-- count(distinct lalvoterid) step. This model scans int__l2_nationwide_uniform once --
-- the "scanned, never surfaced" node from TDD 7.1 -- and emits ONLY block-grain counts
-- (PII-free). It is a model, not an inline CTE, because the ~230M-row unpivot is heavy
-- (Dan's "break out only if it grows complex" clause); it restores the original TDD
-- DAG.
--
-- Scoped to the SAME district types as the substrate
-- (get_l2_major_district_columns),
-- so count-once <= count-multiple holds for the substrate's curated type set. state is
-- derived from the block's geoid FIPS prefix (the substrate's rule), matching the
-- substrate block-for-block; the '06'=6 numeric coercion is proven (the merged
-- substrate
-- has all 51 states, 0 null state). served_set: 'all' (any cohort district) or one of
-- the
-- substrate l2 types. Statewide officials are NOT here -- they are read from the
-- exact T5
-- 'State'
-- rows in people_served and reported separately (TDD 4.5), avoiding a basis mix.
with
    cohort_districts as (
        select distinct
            state,
            l2_district_type as district_type,
            normalized_district_name as district_name
        from {{ ref("int__serve_district_resolution") }}
        where resolution_path != 'unresolved' and not is_statewide
    ),

    state_fips as (
        select fips_code, place_name as state_postal_code
        from {{ ref("fips_codes") }}
        where level = 'state'
    ),

    l2 as (
        select
            lpad(
                cast(residence_addresses_complete_census_geocode as string), 15, '0'
            ) as block_geoid,
            lalvoterid,
            {{ get_l2_major_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
        where residence_addresses_complete_census_geocode is not null
    ),

    unpivoted as (
        select
            block_geoid,
            lalvoterid,
            district_column_name as district_type,
            {{ normalize_l2_district_name("district_value") }} as district_name
        from
            l2 unpivot (
                district_value for district_column_name
                in ({{ get_l2_major_district_columns(use_backticks=false) }})
            )
        where
            district_value is not null
            and trim(district_value) != ''
            -- drop values that normalize to empty (e.g. a bare "(EST.)"), mirroring
            -- int__l2_block_district_map so the voter grain (and thus total_voters)
            -- stays identical to the substrate across refreshes (verified 0-diff today)
            and trim({{ normalize_l2_district_name("district_value") }}) != ''
    ),

    -- per (block, voter, type): is this voter in a cohort district of this type?
    voter_type as (
        select
            u.block_geoid,
            u.lalvoterid,
            u.district_type,
            max(
                case when c.district_type is not null then 1 else 0 end
            ) as served_this_type
        from unpivoted u
        join state_fips sf on left(u.block_geoid, 2) = sf.fips_code
        left join
            cohort_districts c
            on c.state = sf.state_postal_code
            and c.district_type = u.district_type
            and c.district_name = u.district_name
        group by u.block_geoid, u.lalvoterid, u.district_type
    ),

    -- per (block, voter): served by ANY cohort district (the cross-type union)
    voter_any as (
        select block_geoid, lalvoterid, max(served_this_type) as served_any
        from voter_type
        group by block_geoid, lalvoterid
    ),

    -- block denominator + the cohort-wide served-voter union
    block_all as (
        select block_geoid, count(*) as total_voters, sum(served_any) as served_voters
        from voter_any
        group by block_geoid
    ),

    -- per (block, type): voters served by a cohort district of that type
    block_by_type as (
        select
            block_geoid,
            district_type as served_set,
            sum(served_this_type) as served_voters
        from voter_type
        group by block_geoid, district_type
        having sum(served_this_type) > 0
    ),

    coverage as (
        select block_geoid, 'all' as served_set, served_voters, total_voters
        from block_all
        where served_voters > 0

        union all

        select t.block_geoid, t.served_set, t.served_voters, b.total_voters
        from block_by_type t
        join block_all b on t.block_geoid = b.block_geoid
    )

select block_geoid, served_set, served_voters, total_voters
from coverage
