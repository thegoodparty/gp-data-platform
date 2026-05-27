{{
    config(
        warn_if="> 517",
        error_if="> 1034",
    )
}}

-- B3: warn if >1% of TS candidacies are missing viability_score,
-- error if >2%. Empirical MLflow floor is ~0.3%, so 1% absorbs
-- legitimate skips plus small ingestion drift. Default severity
-- 'error' (not 'warn') so error_if produces an actual error.
select gp_candidacy_id
from {{ ref("candidacy") }}
where array_contains(source_systems, 'techspeed') and viability_score is null
