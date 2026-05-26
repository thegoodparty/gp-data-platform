{{
    config(
        warn_if=">15524",
        error_if=">25874",
    )
}}

-- B3: PR-1 baseline guard on absolute counts of TS candidacies missing
-- viability_score. Thresholds pegged at the PR-1 TS row count of ~51,748:
-- warn_if=15524  (~30% of baseline)
-- error_if=25874 (~50% of baseline)
-- PR 2 (Task 12) recomputes both integers against the then-current row
-- count and tightens to ~1%/~2% after the parser fix lifts coverage to
-- the MLflow ceiling. Absolute counts are intentional here: a percentage-
-- based denominator would silently absorb a regression as TS volume grows.
--
-- Default severity is 'error' (not 'warn'). Per dbt's severity reference,
-- default 'error' evaluates error_if first, then warn_if; setting
-- severity='warn' would cap max severity at warn and skip the error gate.
select gp_candidacy_id
from {{ ref("candidacy") }}
where array_contains(source_systems, 'techspeed') and viability_score is null
