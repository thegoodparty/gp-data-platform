{{ config(severity="error") }}

-- The 2020 decennial census frame is static: the national block count and
-- total population (including Puerto Rico) are exact, stable targets, so a
-- fixed-count test is appropriate here, and only here; do not copy this
-- pattern to refreshing sources. Validated against official 2020 census
-- tallies (DATA-1359 TDD, appendix D.1).
with
    totals as (
        select count(*) as total_blocks, sum(population) as total_population
        from {{ ref("stg_nhgis__block_population") }}
    )
select *
from totals
where total_blocks != 8174955 or total_population != 334735155
