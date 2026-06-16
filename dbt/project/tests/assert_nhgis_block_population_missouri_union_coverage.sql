{{ config(severity="error") }}

-- Missouri spans BOTH NHGIS source files: the a/b split is by row count, not
-- state, so a union that silently drops either source corrupts Missouri
-- first. The 2020 decennial frame is static, which makes the exact official
-- block count a stable regression target (validated against the official
-- 2020 census tally; DATA-1359 TDD, appendix D.1).
with
    missouri as (
        select count(*) as mo_blocks
        from {{ ref("stg_nhgis__block_population") }}
        where state_fips = '29'
    )
select *
from missouri
where mo_blocks != 253632
