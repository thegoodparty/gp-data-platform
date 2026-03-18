-- Civics mart candidate table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
-- Deduplicates on gp_candidate_id (a person may appear in both sources)
with
    combined as (
        select *
        from {{ ref("int__civics_candidate_2025") }}
        union all
        select *
        from {{ ref("int__civics_candidate_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidate_id order by updated_at desc)
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
