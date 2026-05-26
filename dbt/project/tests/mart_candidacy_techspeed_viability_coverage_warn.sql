{{
    config(
        warn_if=">15524",
        error_if=">25874",
    )
}}

-- B3: warn if >30% of TS candidacies are missing viability_score (PR 1
-- baseline), error if >50%. PR 2 tightens these to 1%/2% once parsing
-- closes the candidate_code mismatch and coverage approaches MLflow ceiling.
--
-- Default severity is 'error' (not 'warn') so error_if produces an actual
-- error rather than being capped at warn.
select gp_candidacy_id
from {{ ref("candidacy") }}
where array_contains(source_systems, 'techspeed') and viability_score is null
