{{
    config(
        materialized="table",
    )
}}

-- Civics mart candidate table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
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

from {{ ref("candidate_20260122") }}
