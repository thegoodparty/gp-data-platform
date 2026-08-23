-- An office holding a match to a district that still exists must not come
-- back onto the pending list just because L2 shipped a new file for some
-- other state -- that is what keeps a daily run bounded to the unmatched
-- pool instead of all ~286,000 offices. If this returns rows,
-- int__l2_br_match_pending_offices resurrected an office whose latest
-- completed attempt is still a live match, silently reopening the exact
-- failure this model exists to close.
with
    completed_attempts as (
        select
            results.br_database_id,
            results.match_status,
            results.l2_state,
            results.l2_district_type,
            results.l2_district_name,
            results.attempted_at,
            runs.sequence
        from {{ source("model_predictions", "llm_l2_br_match_results") }} as results
        inner join
            {{ source("model_predictions", "llm_l2_br_match_runs") }} as runs
            on results.run_id = runs.run_id
        where runs.status = 'COMPLETE'
    ),
    -- This ordering must match int__l2_br_match_pending_offices exactly.
    -- sequence belongs to the run, so one run's rows all share it, and the
    -- results table is append-only with no key: a mid-run retry can leave two
    -- rows for one office at the same sequence. If the model and this test broke
    -- that tie differently, the test would red against a correct table.
    latest_attempt as (
        select
            br_database_id,
            match_status,
            l2_state,
            l2_district_type,
            l2_district_name,
            sequence
        from completed_attempts
        qualify
            row_number() over (
                partition by br_database_id
                order by sequence desc, attempted_at desc, match_status
            )
            = 1
    ),
    still_valid_matches as (
        select latest_attempt.br_database_id
        from latest_attempt
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as office
            on latest_attempt.br_database_id = office.database_id
        inner join
            {{ ref("int__l2_district_universe") }} as universe
            on latest_attempt.l2_state = universe.state_postal_code
            and latest_attempt.l2_state = office.state
            and latest_attempt.l2_district_type = universe.district_type
            and latest_attempt.l2_district_name = universe.district_name
        where latest_attempt.match_status = 'MATCHED'
    )
select pending.br_database_id
from {{ ref("int__l2_br_match_pending_offices") }} as pending
inner join
    still_valid_matches on pending.br_database_id = still_valid_matches.br_database_id
