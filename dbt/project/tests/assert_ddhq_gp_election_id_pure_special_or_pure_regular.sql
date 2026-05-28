{{ config(severity="warn", warn_if=">5", error_if=">25") }}

-- A single gp_election_id should contain only special stages or only regular
-- stages. Real-world hits are typically upstream ER clustering issues
-- (int__civics_er_canonical_ids joining a special and regular race for the
-- same office+date), which override the hash-distinct gp_election_ids the
-- macro emits. Warn at >5 to surface emerging ER drift; error at >25 to
-- block runaway regressions.
select gp_election_id
from {{ ref("int__civics_candidacy_stage_ddhq") }}
group by gp_election_id
having
    bool_or(election_stage like '%special%')
    and bool_or(election_stage not like '%special%')
