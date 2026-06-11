-- DDHQ vs BallotReady local election coverage gaps.
--
-- This is a dbt *analysis*: it is compiled (refs resolved, validated against the
-- schema) but never materialized. Run the compiled SQL ad hoc to inspect which
-- BallotReady local races DDHQ has not matched.
--
-- Definition of a gap: BallotReady has the race (`ballotready` in source_systems)
-- but DDHQ does not (`ddhq` not in source_systems). Scoped to be a fair,
-- actionable comparison:
-- - bounded to DDHQ's data horizon (max DDHQ election_date) so far-future BR
-- races DDHQ has not loaded yet are not counted as gaps;
-- - local only (office_level not in State/Federal);
-- - partisan primaries excluded (out of scope for the DDHQ coverage agreement:
-- independent / 3rd-party candidates are ineligible);
-- - real races only (br_candidate_count > 0) -- races with no filed candidates
-- are largely uncontested or BR implied-future races that never reach DDHQ.
-- has_techspeed_match flags the highest-confidence subset: a second source
-- independently confirms the race exists.
with
    ddhq_horizon as (
        select max(election_date) as last_ddhq_election_date
        from {{ ref("int__civics_election_stage_ddhq") }}
    )

select
    e.gp_election_stage_id,
    -- 2-letter state prefix of race_name (reliable for 2026+ rows).
    upper(substring(e.race_name, 1, 2)) as state,
    e.race_name,
    e.office_level,
    e.election_date,
    e.stage_type,
    e.br_candidate_count,
    array_contains(e.source_systems, 'techspeed') as has_techspeed_match
from {{ ref("election_stage") }} as e
cross join ddhq_horizon as h
where
    not array_contains(e.source_systems, 'ddhq')
    and array_contains(e.source_systems, 'ballotready')
    and e.election_date between date '2026-01-01' and h.last_ddhq_election_date
    and e.office_level not in ('State', 'Federal')
    and not coalesce(e.partisan_type = 'partisan' and e.is_primary, false)
    and e.br_candidate_count > 0
order by state, e.election_date, e.race_name
