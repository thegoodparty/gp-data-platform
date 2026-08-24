-- Explicit because this directory sets no materialization default: an empty
-- config block would silently make this a view, and a view would recompute
-- "the pending list" on every read instead of pinning the batch a run reads.
{{ config(materialized="table") }}

/*
Reads stg_airbyte_source__ballotready_api_position directly, not
int__enhanced_position, which drops sub_area_name, sub_area_value, is_judicial
and has_unknown_boundaries -- exactly the geography fields the matcher's
menu-narrowing filters need.

llm_l2_br_match_runs and llm_l2_br_match_results are sources, not refs:
provisioned outside dbt (dbt/scripts/llm_l2_br_match_tables.sql) and written
only by the matcher.

This is a table, so it is a snapshot of the worklist as of its last build. The
supervised cutover MUST rebuild it after seeding the baseline run, or it still
holds every office and hands the matcher ~286,000 instead of the ~23,000
backlog.
*/
with
    br_offices as (
        select
            database_id as br_database_id,
            name,
            mtfcc,
            geo_id,
            sub_area_name,
            sub_area_value,
            is_judicial,
            has_unknown_boundaries,
            state
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    -- Run status is what makes a result real: RUNNING and REVOKED rows must
    -- not resolve an office one way or the other.
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

    -- "Current means latest": the newest row per office ordered by run
    -- sequence, never by confidence or match_status, so a newer abstention
    -- supersedes an older match.
    --
    -- The tiebreak past sequence is load-bearing, not defensive. sequence
    -- belongs to the run, so every result row in one run shares it, and the
    -- results table is append-only with no key -- a mid-run retry can leave two
    -- rows for one office at the same sequence. Ordering on sequence alone then
    -- picks arbitrarily, and the singular test recomputes this same window in a
    -- separate query, so the two can disagree and red the build against a
    -- correctly built table. Keep both orderings identical.
    latest_attempt as (
        select
            br_database_id,
            match_status,
            l2_state,
            l2_district_type,
            l2_district_name,
            attempted_at,
            sequence
        from completed_attempts
        qualify
            row_number() over (
                partition by br_database_id
                order by sequence desc, attempted_at desc, match_status
            )
            = 1
    ),

    -- Rule 3 keys on the absence of a universe row, which cannot by itself tell
    -- "this district was relabelled" from "this state has no universe rows right
    -- now". A stalled L2 load would flood every matched office in the state back
    -- onto the list, which is the unbounded-run failure this model exists to
    -- prevent.
    states_in_universe as (
        select distinct state_postal_code from {{ ref("int__l2_district_universe") }}
    )

select
    br_offices.br_database_id,
    br_offices.name,
    br_offices.mtfcc,
    br_offices.geo_id,
    br_offices.sub_area_name,
    br_offices.sub_area_value,
    br_offices.is_judicial,
    br_offices.has_unknown_boundaries,
    br_offices.state
from br_offices
left join latest_attempt on br_offices.br_database_id = latest_attempt.br_database_id
-- Rule 3's join. A match is live only if the district it named still exists in
-- the state it was matched in AND the office is still in that state, so both
-- equalities on l2_state are load-bearing: a district key here is
-- (state, type, name) exactly as everywhere else in this repo, and a position
-- that changes state must not inherit a same-named district in the new one.
left join
    {{ ref("int__l2_district_universe") }} as universe
    on latest_attempt.l2_state = universe.state_postal_code
    and latest_attempt.l2_state = br_offices.state
    and latest_attempt.l2_district_type = universe.district_type
    and latest_attempt.l2_district_name = universe.district_name
where
    -- Rule 1: never attempted.
    latest_attempt.br_database_id is null
    -- Rule 2: last attempt found nothing, and that was over 30 days ago. The
    -- 30 is a design constant from the sign-off, not a tunable.
    or (
        latest_attempt.match_status = 'ABSTAINED'
        and latest_attempt.attempted_at < current_date() - interval 30 days
    )
    -- Rule 3: matched, but the universe rebuilt and no longer carries that
    -- exact label -- the one join that stops a dead match sitting here
    -- indefinitely. An office matched to a district that still exists must
    -- NOT reappear just because L2 shipped a new file for some other state,
    -- nor because its own state is momentarily absent from the universe.
    or (
        latest_attempt.match_status = 'MATCHED'
        and universe.district_name is null
        and br_offices.state in (select state_postal_code from states_in_universe)
    )
    -- Fail safe. The three rules above enumerate the only statuses the contract
    -- allows, so an unexpected one would fall through all of them and retire the
    -- office silently, forever, with nothing failing. An unknown outcome is not
    -- an outcome: re-attempt it. (A null status is rule 1's case; `not in` on
    -- null yields unknown, so this does not double-count a never-attempted
    -- office.)
    or latest_attempt.match_status not in ('MATCHED', 'ABSTAINED')
