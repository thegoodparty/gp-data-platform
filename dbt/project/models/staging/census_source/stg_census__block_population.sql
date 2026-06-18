-- Named for the census *concept* (decennial block population), not its current
-- ingestion path: v1 still reads the research-staged NHGIS extract via
-- source('nhgis', ...). A documented fast-follow swaps the source to a direct
-- Census decennial pull (byte-identical output), and a census-concept model
-- name keeps that swap from churning every downstream ref(). Deliberate, not a smell.
--
-- 2020 decennial census population per census block, unioned from the two
-- research-staged NHGIS extracts. The a/b file split is by row count, not
-- state (Missouri spans both files), so both sources are required for a
-- complete national frame. Block GEOIDs are zero-padded to the canonical
-- 15 characters: numeric storage drops leading zeros, and downstream joins
-- (e.g. to L2's numeric census geocode) assume the padded form.
with
    pop_a as (
        select
            -- the inner bigint casts pin decimal string output even if a
            -- re-staged source lands with float-inferred numeric columns
            lpad(cast(cast(geocode as bigint) as string), 15, '0') as block_geoid,
            lpad(cast(cast(statea as bigint) as string), 2, '0') as state_fips,
            cast(u7h001 as bigint) as population
        from {{ source("nhgis", "blocks_to_pop_a") }}
        -- this table is all-string and embeds a descriptive header as its
        -- first data row; keep only rows whose state code is numeric
        where try_cast(statea as int) is not null
    ),

    pop_b as (
        select
            lpad(cast(cast(geocode as bigint) as string), 15, '0') as block_geoid,
            lpad(cast(cast(statea as bigint) as string), 2, '0') as state_fips,
            cast(u7h001 as bigint) as population
        from {{ source("nhgis", "blocks_to_pop_b") }}
    ),

    unioned as (
        select *
        from pop_a
        union all
        select *
        from pop_b
    ),

    final as (select *, left(block_geoid, 11) as tract_geoid from unioned)

select *
from final
