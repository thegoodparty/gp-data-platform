{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidate schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates
-- Deduplicates on person-level identity (name + state + contact info)
--
-- CRITICAL: UUID fields MUST match int__civics_candidate_2025.sql pattern
-- to ensure same person from different sources gets same gp_candidate_id
with
    -- ER crosswalk: for clustered TS candidacies, adopt BR's canonical candidate_id.
    -- Deduped per source_candidate_id (one person may have multiple candidacies).
    canonical_candidate as (
        select ts_source_candidate_id, canonical_gp_candidate_id
        from {{ ref("int__civics_er_canonical_ids") }}
        qualify
            row_number() over (
                partition by ts_source_candidate_id order by canonical_gp_candidate_id
            )
            = 1
    ),

    source as (
        select
            ts.*,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    candidates_with_id as (
        -- If ANY candidacy of a person was clustered to BR, all raw rows for
        -- that person adopt BR's canonical_gp_candidate_id (propagated via a
        -- window partitioned by the raw TS hash). Otherwise keep the TS hash.
        select
            coalesce(
                max(xw.canonical_gp_candidate_id) over (
                    partition by {{ generate_ts_gp_candidate_id() }}
                ),
                {{ generate_ts_gp_candidate_id() }}
            ) as gp_candidate_id,

            cast(null as string) as hubspot_contact_id,
            cast(null as string) as prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,
            birth_date_parsed as birth_date,
            state_postal_code as state,
            email,
            phone as phone_number,
            street_address,
            website_url,
            linkedin_url,
            twitter_handle,
            facebook_url,
            instagram_handle,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        left join
            canonical_candidate as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
        where first_name is not null and last_name is not null and state is not null
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
