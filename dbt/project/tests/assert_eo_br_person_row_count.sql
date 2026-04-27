{{ config(severity="error") }}

-- Asserts BR person rollup row count equals distinct br_candidate_id with
-- at least one non-vacancy term in the BR intermediate. Reference: 375,321
-- rows in PR #333 CI data (2026-04-27).
with
    expected as (
        select count(distinct br_candidate_id) as n
        from {{ ref("int__civics_elected_official_ballotready") }}
        where br_candidate_id is not null
    ),
    actual as (
        select count(*) as n
        from {{ ref("int__civics_elected_official_ballotready_person") }}
    )
select expected.n as expected_rows, actual.n as actual_rows
from expected, actual
where expected.n != actual.n
