{{
    config(
        materialized="table", tags=["intermediate", "l2", "districts", "substrate"]
    )
}}

-- int__l2_block_district_map: one row per (census block, district_type,
-- normalized district_name) with the L2 voter count in that intersection
-- (voters_in_block_district) and the per-(block, type) voter total
-- (voters_in_block). The block-grain twin of int__zip_code_to_l2_district and
-- the input to int__district_census_allocation (the substrate, DATA-1992).
--
-- UNPIVOTs the curated substrate district columns
-- (get_l2_major_district_columns) -- the cohort-occupied office-bearing types
-- (widened in DATA-2013). District names are normalized here so the grain and
-- every downstream name-join key match the serve resolver's
-- normalized_district_name (L2 "(EST.)"/whitespace drift between snapshots).
--
-- state_postal_code is derived from the block's geoid FIPS prefix
-- (block_geoid[:2]) via the fips_codes seed, NOT from L2's per-voter
-- state_postal_code. A handful of voters are geocoded across a state line
-- (their L2 state disagrees with the block's geoid); the census geoid is the
-- source of truth for which state a block is in. Deriving from the geoid also
-- keeps state functionally determined by block_geoid, so the (block, type,
-- name) grain stays unique (no cross-state duplicate rows).
--
-- Block grain is the source of truth for overlap. Do NOT count by joining the
-- district-level int__l2_district_aggregations on the normalized name: its
-- (state, type, name) key is non-unique and would fan out (DATA-1988 codex #5).
with
    l2 as (
        select
            -- L2 stores the 15-digit block geocode as a LONG, so leading zeros
            -- are dropped; LPAD back to the canonical padded block_geoid before
            -- any join to stg_census__block_population.
            lpad(
                cast(residence_addresses_complete_census_geocode as string), 15, '0'
            ) as block_geoid,
            lalvoterid,
            loaded_at,
            {{ get_l2_major_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
        where residence_addresses_complete_census_geocode is not null
    ),

    unpivoted as (
        select
            block_geoid,
            lalvoterid,
            loaded_at,
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
            -- drop values that normalize to empty (e.g. a bare "(EST.)"), which
            -- would otherwise pass not_null as an empty district_name
            and trim({{ normalize_l2_district_name("district_value") }}) != ''
    ),

    block_district as (
        select
            block_geoid,
            district_type,
            district_name,
            count(distinct lalvoterid) as voters_in_block_district,
            max(loaded_at) as loaded_at
        from unpivoted
        group by block_geoid, district_type, district_name
    ),

    -- map the block's 2-digit geoid FIPS prefix to its state postal code
    state_fips as (
        select fips_code, place_name as state_postal_code
        from {{ ref("fips_codes") }}
        where level = 'state'
    )

select
    bd.block_geoid,
    sf.state_postal_code,
    bd.district_type,
    bd.district_name,
    bd.voters_in_block_district,
    -- per-(block, district_type) denominator for the substrate's split fraction
    sum(bd.voters_in_block_district) over (
        partition by bd.block_geoid, bd.district_type
    ) as voters_in_block,
    bd.loaded_at
from block_district bd
left join state_fips sf on left(bd.block_geoid, 2) = sf.fips_code
