{{
    config(
        materialized="table", tags=["intermediate", "l2", "districts", "substrate"]
    )
}}

-- int__l2_block_district_map: one row per (census block, state, district_type,
-- normalized district_name) with the L2 voter count in that intersection
-- (voters_in_block_district) and the per-(block, type) voter total
-- (voters_in_block). The block-grain twin of int__zip_code_to_l2_district and
-- the input to int__district_census_allocation (the substrate, DATA-1992).
--
-- UNPIVOTs ONLY the v1 major office-bearing district columns
-- (get_l2_district_columns(major_only=true)); the sparse special-district long
-- tail is a documented fast-follow. District names are normalized here so the
-- grain and every downstream name-join key match the serve resolver's
-- normalized_district_name (L2 "(EST.)"/whitespace drift between snapshots).
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
            state_postal_code,
            lalvoterid,
            loaded_at,
            {{ get_l2_major_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
        where residence_addresses_complete_census_geocode is not null
    ),

    unpivoted as (
        select
            block_geoid,
            state_postal_code,
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
            state_postal_code,
            district_type,
            district_name,
            count(distinct lalvoterid) as voters_in_block_district,
            max(loaded_at) as loaded_at
        from unpivoted
        group by block_geoid, state_postal_code, district_type, district_name
    )

select
    block_geoid,
    state_postal_code,
    district_type,
    district_name,
    voters_in_block_district,
    -- per-(block, state, district_type) denominator for the split fraction.
    -- state is in the partition though block->state is 1:1 (block_geoid[:2] =
    -- state FIPS): a no-op on clean data, but it makes a cross-state blend
    -- structurally impossible rather than relying on the guard test alone.
    sum(voters_in_block_district) over (
        partition by block_geoid, state_postal_code, district_type
    ) as voters_in_block,
    loaded_at
from block_district
