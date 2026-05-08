{{
    config(
        severity="warn",
        warn_if=">200",
        error_if=">1000",
    )
}}

-- A candidacy_stage row should not have a populated election_result before
-- the election_stage_date. If we're seeing winners declared before the race
-- happens, the upstream provider has a data freshness or stage-attribution
-- bug. Warn-only because BR/TS sometimes attribute candidate-level results
-- to future stages; the threshold is set to surface trends without blocking.
select gp_candidacy_stage_id, election_stage_date, election_result, is_winner
from {{ ref("candidacy_stage") }}
where
    election_stage_date > current_date()
    and (election_result is not null or is_winner is not null)
