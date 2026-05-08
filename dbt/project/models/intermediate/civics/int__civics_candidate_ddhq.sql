-- DDHQ → Civics mart candidate
-- Derived from: int__civics_candidacy_stage_ddhq
--
-- Grain: One row per unique person (deduplicated on gp_candidate_id)
--
-- CRITICAL: UUID fields MUST match int__civics_candidate_2025.sql pattern
-- to ensure same person from different sources gets same gp_candidate_id
with
    deduplicated as (
        select *
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
        qualify
            row_number() over (partition by gp_candidate_id order by updated_at desc)
            = 1
    )

select
    gp_candidate_id,
    cast(null as string) as hubspot_contact_id,
    cast(null as string) as prod_db_user_id,
    cast(null as string) as candidate_id_tier,
    candidate_first_name as first_name,
    candidate_last_name as last_name,
    candidate_full_name as full_name,
    cast(null as date) as birth_date,
    state,
    state_postal_code,
    cast(null as string) as email,
    cast(null as string) as phone_number,
    cast(null as string) as street_address,
    cast(null as string) as website_url,
    cast(null as string) as linkedin_url,
    cast(null as string) as twitter_handle,
    cast(null as string) as facebook_url,
    cast(null as string) as instagram_handle,
    created_at,
    updated_at
from deduplicated
