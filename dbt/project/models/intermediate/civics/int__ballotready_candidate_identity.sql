-- Canonical BallotReady candidate identity inputs, one row per br_candidate_id.
-- Picks one deterministic identity value per br_candidate_id so both consumers
-- (int__civics_candidate_ballotready, int__civics_candidacy_ballotready) hash
-- identical gp_candidate_id inputs; otherwise a person resolves to different
-- ids and gets orphaned from their candidacies (S3 email/name/phone vary across
-- a person's candidacy rows).
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
