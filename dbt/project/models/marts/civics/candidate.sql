{{
    config(
        materialized="table",
    )
}}

with
    -- Only include candidates who have a candidacy in the civics mart
    -- This ensures referential integrity with the candidacy table
    valid_candidate_ids as (
        select distinct gp_candidate_id
        from {{ ref("candidacy") }}
        where gp_candidate_id is not null
    ),

    archived_candidates as (
        -- Historical archive: candidates from elections on or before 2025-12-31
        select candidate.*
        from {{ ref("m_general__candidate_v2") }} as candidate
        inner join
            valid_candidate_ids
            on candidate.gp_candidate_id = valid_candidate_ids.gp_candidate_id
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
