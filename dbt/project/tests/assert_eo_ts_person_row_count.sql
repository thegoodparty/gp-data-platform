{{ config(severity="error") }}

-- Asserts TS person rollup row count equals the count of distinct canonical
-- IDs reachable through non-reused crosswalk rows. Reference: 27,827 rows
-- in PR #333 CI data (2026-04-27).
with
    expected as (
        select count(distinct xw.canonical_gp_elected_official_id) as n
        from {{ ref("int__civics_elected_official_techspeed") }} as ts
        inner join
            {{ ref("int__civics_elected_official_canonical_ids") }} as xw
            on ts.ts_officeholder_id = xw.ts_officeholder_id
        where not xw.ts_officeholder_id_is_reused
    ),
    actual as (
        select count(*) as n
        from {{ ref("int__civics_elected_official_techspeed_person") }}
    )
select expected.n as expected_rows, actual.n as actual_rows
from expected, actual
where expected.n != actual.n
