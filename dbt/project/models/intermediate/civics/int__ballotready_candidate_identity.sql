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

    -- One coherent representative candidacy row per person: the most recently
    -- updated, with deterministic email/phone tie-breaks (mirrors the ordering
    -- int__civics_candidate_ballotready used for its dedup). Taking a single
    -- row keeps the identity fields internally consistent and uses the person's
    -- freshest contact info, rather than a per-column min() that could stitch
    -- together values from different candidacy rows.
    ranked as (
        select
            br_candidate_id,
            first_name,
            last_name,
            state,
            email as s3_email,
            phone,
            row_number() over (
                partition by br_candidate_id
                order by
                    coalesce(candidacy_updated_at, _airbyte_extracted_at) desc,
                    email asc nulls last,
                    phone asc nulls last
            ) as rn
        from candidacies
    )

-- Columns are id_-prefixed so consumers can join this model without colliding
-- with their own first_name / last_name / state / email / phone columns.
select
    ranked.br_candidate_id,
    ranked.first_name as id_first_name,
    ranked.last_name as id_last_name,
    ranked.state as id_state,
    -- Prefer the representative row's S3 email, fall back to the API person
    -- email, matching the coalesce(email, api_email) the consumers used before.
    coalesce(ranked.s3_email, person_emails.api_email) as id_email,
    ranked.phone as id_phone
from ranked
left join person_emails on ranked.br_candidate_id = person_emails.person_database_id
where ranked.rn = 1
