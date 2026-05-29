{{ config(severity="warn", warn_if=">5", error_if=">25") }}

-- BR counterpart of assert_ddhq_special_vs_regular_distinct_gp_election_id.
-- Verifies stg_airbyte_source__ballotready_api_election.is_special is wired
-- correctly through int__civics_election_stage_ballotready into the
-- gp_election_id hash.
--
-- Each row in int__civics_election_stage_ballotready is one stage. We do NOT
-- pre-aggregate is_special per gp_election_id: a true collision would yield
-- one stage row with is_special=true and another with is_special=false sharing
-- the same gp_election_id, and that's the signal we need preserved at the
-- outer group level for bool_or to detect. Warn-only because real-world hits
-- typically reflect upstream BR data drift (election.name not consistently
-- marked 'Special') rather than a hash regression.
select br_position_id, election_year
from
    (
        select
            gp_election_id,
            br_position_id,
            year(election_date) as election_year,
            is_special
        from {{ ref("int__civics_election_stage_ballotready") }}
    ) as stages
group by br_position_id, election_year
having
    bool_or(is_special)
    and bool_or(not is_special)
    and count(distinct gp_election_id) < 2
