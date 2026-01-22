{{
    config(
        materialized="table",
    )
}}

with
    -- Only include candidates who have a candidacy with valid election dates
    -- Filter out invalid dates (e.g., years like 0028, 1024 which are data entry
    -- errors)
    -- This ensures consistency with the candidacy table filtering
    valid_candidates as (
        select distinct hubspot_contact_id
        from {{ ref("m_general__candidacy_v2") }}
        where
            general_election_date <= '2025-12-31'
            and general_election_date >= '1900-01-01'
    ),

    archived_candidates as (
        -- Historical archive: candidates from elections on or before 2025-12-31
        select candidate.*
        from {{ ref("m_general__candidate_v2") }} as candidate
        inner join
            valid_candidates
            on candidate.hubspot_contact_id = valid_candidates.hubspot_contact_id
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

from archived_candidates
