-- Canonical BallotReady candidate name/contact/party per br_candidate_id: one
-- deterministic representative row, since S3 email/name/phone vary across a
-- person's candidacy rows. Feeds the person mart's display attributes only.
-- All election years: is_candidate is flagged all-time, so a date filter here
-- left older candidates nameless and dropped them from the public profiles.
with
    candidacies as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
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

    ranked as (
        select
            br_candidate_id,
            first_name,
            last_name,
            state,
            email as s3_email,
            phone,
            parties,
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
    ranked.phone as id_phone,
    ranked.parties as id_party
from ranked
left join person_emails on ranked.br_candidate_id = person_emails.person_database_id
where ranked.rn = 1
