-- BallotReady candidates → Civics mart candidate schema. Grain: one row per person.
-- UUID fields MUST match int__civics_candidate_2025 so the same person from
-- different sources resolves to the same gp_candidate_id.
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    br_person as (select * from {{ ref("int__ballotready_person") }}),

    person_urls as (
        select
            database_id as person_database_id,
            first_name,
            last_name,
            full_name,
            middle_name,
            nickname,
            suffix,
            get(filter(urls, x -> x.type = 'website'), 0).url as website_url,
            get(filter(urls, x -> x.type = 'linkedin'), 0).url as linkedin_url,
            get(filter(urls, x -> x.type = 'twitter'), 0).url as twitter_url,
            get(filter(urls, x -> x.type = 'facebook'), 0).url as facebook_url,
            get(filter(urls, x -> x.type = 'instagram'), 0).url as instagram_url,
            get(filter(contacts, x -> x.email is not null), 0).email as api_email,
            nullif(
                regexp_replace(
                    get(filter(contacts, x -> x.phone is not null), 0).phone, '-', ''
                ),
                ''
            ) as api_phone,
            created_at as person_created_at,
            updated_at as person_updated_at
        from br_person
        where database_id is not null
    ),

    candidates_with_id as (
        select
            -- Primary identifier - matches HubSpot/TechSpeed pattern
            -- HubSpot uses: first_name, last_name, state, birth_date, email,
            -- phone_number
            -- BallotReady has no birth_date, so it will be NULL (coalesced to '' by
            -- macro)
            -- Hash the canonical per-person identity inputs (one deterministic
            -- value per br_candidate_id) so this model and
            -- int__civics_candidacy_ballotready resolve a person to the same
            -- gp_candidate_id. Hashing per-row contact fields here split a
            -- person with varying email across candidacies into multiple ids.
            {{
                generate_salted_uuid(
                    fields=[
                        "identity.id_first_name",
                        "identity.id_last_name",
                        "identity.id_state",
                        "cast(null as string)",
                        "identity.id_email",
                        "identity.id_phone",
                    ]
                )
            }} as gp_candidate_id,

            -- BR person database_id (S3 candidacies carry this as br_candidate_id).
            -- Exposed so downstream marts can map a BR candidacy row to its
            -- canonical gp_candidate_id without re-running the salted-UUID macro.
            candidacies.br_candidate_id,

            -- External IDs (NULL for BR-sourced)
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as prod_db_user_id,

            cast(null as string) as candidate_id_tier,

            -- Name fields - prefer API person data if available
            coalesce(person_urls.first_name, candidacies.first_name) as first_name,
            coalesce(person_urls.last_name, candidacies.last_name) as last_name,
            coalesce(
                person_urls.full_name,
                concat(candidacies.first_name, ' ', candidacies.last_name)
            ) as full_name,

            cast(null as date) as birth_date,
            candidacies.state,

            -- Contact info - prefer S3 data, fall back to API
            coalesce(candidacies.email, person_urls.api_email) as email,
            coalesce(candidacies.phone, person_urls.api_phone) as phone_number,
            cast(null as string) as street_address,

            person_urls.website_url,
            person_urls.linkedin_url as linkedin_url,
            person_urls.twitter_url as twitter_handle,
            person_urls.facebook_url as facebook_url,
            person_urls.instagram_url as instagram_handle,

            coalesce(
                person_urls.person_created_at, candidacies._airbyte_extracted_at
            ) as created_at,
            coalesce(
                person_urls.person_updated_at, candidacies._airbyte_extracted_at
            ) as updated_at

        from candidacies
        left join
            person_urls on candidacies.br_candidate_id = person_urls.person_database_id
        left join
            {{ ref("int__ballotready_candidate_identity") }} as identity
            on candidacies.br_candidate_id = identity.br_candidate_id
    ),

    deduplicated as (
        select *
        from candidates_with_id
        qualify
            row_number() over (
                partition by gp_candidate_id
                order by
                    updated_at desc, email asc nulls last, phone_number asc nulls last
            )
            = 1
    )

select
    gp_candidate_id,
    br_candidate_id,
    hubspot_contact_id,
    prod_db_user_id,
    candidate_id_tier,
    first_name,
    last_name,
    full_name,
    birth_date,
    state,
    email,
    phone_number,
    street_address,
    website_url,
    linkedin_url,
    twitter_handle,
    facebook_url,
    instagram_handle,
    created_at,
    updated_at
from deduplicated
