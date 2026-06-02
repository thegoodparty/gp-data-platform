-- Canonical BallotReady candidate identity inputs, one row per br_candidate_id.
--
-- gp_candidate_id is a salted UUID over (first_name, last_name, state,
-- birth_date, email, phone). BallotReady S3 data is at the candidacy-stage
-- grain, and a person's email (occasionally name/phone) varies across their
-- candidacy rows. Both int__civics_candidate_ballotready and
-- int__civics_candidacy_ballotready feed gp_candidate_id, and each previously
-- picked its own representative value (per-row vs any_value over a rollup), so
-- the same person resolved to different ids and the candidate was orphaned
-- from its candidacies. This model picks one deterministic value per
-- br_candidate_id so both consumers hash identical inputs.
--
-- Grain: one row per br_candidate_id.
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    -- Per-person API email (keyed by br_candidate_id); used as the fallback
    -- when a candidate's S3 candidacy rows carry no email.
    person_emails as (
        select
            database_id as person_database_id,
            get(filter(contacts, x -> x.email is not null), 0).email as api_email
        from {{ ref("int__ballotready_person") }}
        where database_id is not null
    ),

    -- One deterministic value per person. min() is order-independent and skips
    -- nulls, so it is stable across runs regardless of candidacy-row ordering.
    agg as (
        select
            br_candidate_id,
            min(first_name) as first_name,
            min(last_name) as last_name,
            min(state) as state,
            min(email) as s3_email,
            min(phone) as phone
        from candidacies
        group by br_candidate_id
    )

-- Columns are id_-prefixed so consumers can join this model without colliding
-- with their own first_name / last_name / state / email / phone columns.
select
    agg.br_candidate_id,
    agg.first_name as id_first_name,
    agg.last_name as id_last_name,
    agg.state as id_state,
    -- Prefer the deterministic S3 email, fall back to the API person email,
    -- matching the coalesce(email, api_email) the consumers used before.
    coalesce(agg.s3_email, person_emails.api_email) as id_email,
    agg.phone as id_phone
from agg
left join person_emails on agg.br_candidate_id = person_emails.person_database_id
