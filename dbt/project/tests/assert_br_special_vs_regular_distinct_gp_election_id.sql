{{ config(severity="error", error_if=">0") }}

-- BR counterpart of assert_ddhq_special_vs_regular_distinct_gp_election_id.
-- Verifies stg_airbyte_source__ballotready_api_election.is_special wired
-- correctly through int__civics_election_stage_ballotready into the
-- gp_election_id hash.
with
    elections as (
        select
            gp_election_id,
            br_position_id,
            year(election_date) as election_year,
            max(stage_type like '%special%') as is_special
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id, br_position_id, year(election_date)
    )

select br_position_id, election_year
from elections
group by br_position_id, election_year
having
    bool_or(is_special)
    and bool_or(not is_special)
    and count(distinct gp_election_id) < 2
