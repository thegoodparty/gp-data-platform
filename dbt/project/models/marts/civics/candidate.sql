{{
    config(
        materialized="table",
        tags=["mart", "civics", "historical"],
    )
}}

with
    archived_candidates as (
        -- Historical archive: candidates from elections on or before 2025-12-31
        select candidate.*
        from {{ ref("m_general__candidate_v2") }} as candidate
        inner join
            {{ ref("m_general__candidacy_v2") }} as candidacy
            on candidate.hubspot_contact_id = candidacy.hubspot_contact_id
        where candidacy.general_election_date <= '2025-12-31'
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
