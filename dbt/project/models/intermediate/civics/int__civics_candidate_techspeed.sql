{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidate schema
-- Deduplicates on person-level identity (name + state + contact info)
--
-- CRITICAL: UUID fields MUST match int__civics_candidate_2025.sql pattern
-- to ensure same person from different sources gets same gp_candidate_id
with
    source as (
        select
            *,
            -- Parse birth_date from multiple formats for UUID consistency
            -- Note: Many Jan 1 dates are placeholders, but we include for HubSpot
            -- pattern alignment
            coalesce(
                try_cast(birth_date as date),
                try_to_date(birth_date, 'MM-dd-yyyy'),
                try_to_date(birth_date, 'MM/dd/yyyy'),
                try_to_date(birth_date, 'yyyy-MM-dd')
            ) as birth_date_parsed
        from {{ ref("int__techspeed_candidates_clean") }}
    ),

    candidates_with_id as (
        select
            -- Primary identifier
            -- HubSpot: raw fields, birth_date is STRING in ISO format
            -- TechSpeed: parse birth_date to DATE then cast to string for format
            -- normalization
            -- generate_salted_uuid now normalizes NULL inputs to ''
            -- before salting/hashing for consistent ID generation.
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "cast(birth_date_parsed as string)",
                        "email",
                        "phone",
                    ]
                )
            }} as gp_candidate_id,

            -- External IDs (NULL for TS-sourced)
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as prod_db_user_id,

            -- Confidence tier
            candidate_id_tier,

            -- Name fields
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,

            -- Demographics (use pre-parsed value)
            birth_date_parsed as birth_date,
            state,

            -- Contact info
            email,
            phone as phone_number,
            street_address,

            -- Social/web presence
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,

            -- Timestamps
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        where
            -- Minimum required fields
            first_name is not null and last_name is not null and state is not null
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
