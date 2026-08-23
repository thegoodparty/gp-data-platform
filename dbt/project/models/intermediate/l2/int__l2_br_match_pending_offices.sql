-- Explicit because this directory sets no materialization default: an empty
-- config block would silently make this a view, and a view would recompute
-- "the pending list" on every read instead of pinning the batch a run reads.
{{ config(materialized="table") }}

/*
One row per BallotReady office needing an L2 district match attempt: never
attempted, last attempt abstained more than 30 days ago, or matched to a
district the current L2 universe no longer carries. The matcher reads this
list to build its next batch.

Reads stg_airbyte_source__ballotready_api_position directly, not
int__enhanced_position, which drops sub_area_name, sub_area_value,
is_judicial and has_unknown_boundaries -- exactly the geography fields the
matcher's menu-narrowing filters need.

llm_l2_br_match_runs and llm_l2_br_match_results are dbt sources, not refs:
provisioned outside dbt (dbt/scripts/llm_l2_br_match_tables.sql) and written
only by the matcher. This model, and the matcher, are the only two readers
the data contract names for either table.

Table is empty on every office (~286,000) until the matcher has run at
least once: "never attempted" alone matches everything before that, which is
correct, not a bug.
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
    latest_attempt as (
        select
            br_database_id,
            match_status,
            l2_district_type,
            l2_district_name,
            attempted_at,
            sequence
        from completed_attempts
        qualify
            row_number() over (partition by br_database_id order by sequence desc) = 1
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
-- Rule 3's join: does the office's own state still carry this exact label in
-- the current universe? Results rows carry no state column of their own, so
-- the office's state stands in for it. Guarding on match_status='MATCHED'
-- here is belt-and-suspenders (a null l2_district_type/name, true of every
-- unattempted or ABSTAINED office, already fails every equality below), kept
-- for readability: this join exists only to implement rule 3.
left join
    {{ ref("int__l2_district_universe") }} as universe
    on latest_attempt.match_status = 'MATCHED'
    and br_offices.state = universe.state_postal_code
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
    -- NOT reappear just because L2 shipped a new file for some other state.
    or (latest_attempt.match_status = 'MATCHED' and universe.district_name is null)
