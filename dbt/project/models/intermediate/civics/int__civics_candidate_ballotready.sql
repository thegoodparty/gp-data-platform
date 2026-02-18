-- BallotReady candidates → Civics mart candidate schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
-- Augmented with int__ballotready_person for richer contact/URL data from API
--
-- Grain: One row per unique person
--
-- UUID fields MUST match int__civics_candidate_2025 pattern
-- to ensure same person from different sources gets same gp_candidate_id
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    -- Get enriched person data from BallotReady API (has contact info, URLs, bio)
    br_person as (select * from {{ ref("int__ballotready_person") }}),

    -- Extract URL fields from person API data
    person_urls as (
        select
            database_id as person_database_id,
            first_name,
            last_name,
            full_name,
            middle_name,
            nickname,
            suffix,
            -- Extract typed URLs from person.urls array (use get() for safe access)
            get(filter(urls, x -> x.type = 'website'), 0).url as website_url,
            get(filter(urls, x -> x.type = 'linkedin'), 0).url as linkedin_url,
            get(filter(urls, x -> x.type = 'twitter'), 0).url as twitter_url,
            get(filter(urls, x -> x.type = 'facebook'), 0).url as facebook_url,
            get(filter(urls, x -> x.type = 'instagram'), 0).url as instagram_url,
            -- Extract contact info from person.contacts array
            get(filter(contacts, x -> x.email is not null), 0).email as api_email,
            get(filter(contacts, x -> x.phone is not null), 0).phone as api_phone,
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
            {{
                generate_salted_uuid(
                    fields=[
                        "candidacies.first_name",
                        "candidacies.last_name",
                        "candidacies.state",
                        "cast(null as string)",
                        "coalesce(candidacies.email, person_urls.api_email)",
                        "candidacies.phone",
                    ]
                )
            }} as gp_candidate_id,

            -- External IDs (NULL for BR-sourced)
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as prod_db_user_id,

            -- Confidence tier
            cast(null as string) as candidate_id_tier,

            -- Name fields - prefer API person data if available
            coalesce(person_urls.first_name, candidacies.first_name) as first_name,
            coalesce(person_urls.last_name, candidacies.last_name) as last_name,
            coalesce(
                person_urls.full_name,
                concat(candidacies.first_name, ' ', candidacies.last_name)
            ) as full_name,

            -- Demographics (STRING to match 2025 HubSpot model type)
            cast(null as string) as birth_date,
            candidacies.state,

            -- Contact info - prefer S3 data, fall back to API
            coalesce(candidacies.email, person_urls.api_email) as email,
            coalesce(candidacies.phone, person_urls.api_phone) as phone_number,
            cast(null as string) as street_address,

            -- Social/web presence from API person data
            person_urls.website_url,
            person_urls.linkedin_url as linkedin_url,
            person_urls.twitter_url as twitter_handle,
            person_urls.facebook_url as facebook_url,
            person_urls.instagram_url as instagram_handle,

            -- Timestamps
            coalesce(
                person_urls.person_created_at, candidacies._airbyte_extracted_at
            ) as created_at,
            coalesce(
                person_urls.person_updated_at, candidacies._airbyte_extracted_at
            ) as updated_at

        from candidacies
        left join
            person_urls
            on cast(candidacies.br_candidate_id as int) = person_urls.person_database_id
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
