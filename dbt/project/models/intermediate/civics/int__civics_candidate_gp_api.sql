{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB users -> Civics mart candidate schema.
--
-- Grain: One row per Product DB user who has at least one campaign. Serve-only
-- users (campaign_count = 0) are excluded — they are not candidates.
--
-- Schema aligned with int__civics_candidate_ballotready / _techspeed so the
-- downstream mart union can stack this source directly. Fields not tracked in
-- Product DB (birth_date, street_address, social URLs) are null.
--
-- ER canonicalization: look up canonical_gp_candidate_id via the user's latest
-- campaigns on int__civics_er_canonical_ids, then propagate across all users
-- sharing the same person hash via a window (same pattern as
-- int__civics_candidate_techspeed's cross-row cascade).
with
    users_filtered as (
        select user_id, first_name, last_name, email, phone, created_at, updated_at
        from {{ ref("users") }}
        where campaign_count > 0
    ),

    latest_campaigns as (select * from {{ ref("campaigns") }} where is_latest_version),

    -- State: most recent non-demo campaign's campaign_state per user.
    user_state as (
        select user_id, campaign_state as state
        from latest_campaigns
        where not is_demo
        qualify row_number() over (partition by user_id order by created_at desc) = 1
    ),

    -- ER lookup at candidate grain: any canonical_gp_candidate_id from any of
    -- the user's latest campaigns (a campaign_id can map to multiple stages in
    -- the crosswalk; take max — they resolve to the same canonical candidate).
    user_er_canonical as (
        select c.user_id, max(xw.canonical_gp_candidate_id) as canonical_gp_candidate_id
        from latest_campaigns as c
        inner join
            {{ ref("int__civics_er_canonical_ids") }} as xw
            on c.campaign_id = xw.gp_api_campaign_id
        group by c.user_id
    ),

    -- hubspotid is stored in the user's meta_data JSON blob (not exposed on
    -- the users mart). Databricks' `:` JSON path operator is case-insensitive.
    user_hubspot as (
        select id as user_id, meta_data:hubspotid::string as hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    -- candidate_id_tier: Product DB's `campaign.tier` column is WIN/LOSE/TOSSUP
    -- (populated on ~10 of 61k rows), not the HubSpot viability tier concept.
    -- Emit null like int__civics_candidate_ballotready until a real tier source
    -- is wired up for Product DB.
    candidates_pre as (
        select
            u.user_id as prod_db_user_id,
            u.first_name,
            u.last_name,
            us.state,
            u.email,
            u.phone as phone_number,
            uh.hubspot_contact_id,
            cast(null as string) as candidate_id_tier,
            uer.canonical_gp_candidate_id,
            u.created_at,
            u.updated_at
        from users_filtered as u
        left join user_state as us on u.user_id = us.user_id
        left join user_er_canonical as uer on u.user_id = uer.user_id
        left join user_hubspot as uh on u.user_id = uh.user_id
    ),

    candidates_with_id as (
        select
            coalesce(
                max(canonical_gp_candidate_id) over (
                    partition by
                        {{ generate_gp_api_gp_candidate_id(phone="phone_number") }}
                ),
                {{ generate_gp_api_gp_candidate_id(phone="phone_number") }}
            ) as gp_candidate_id,

            hubspot_contact_id,
            prod_db_user_id,
            candidate_id_tier,
            first_name,
            last_name,
            concat(first_name, ' ', last_name) as full_name,
            cast(null as date) as birth_date,
            state,
            email,
            phone_number,
            cast(null as string) as street_address,
            cast(null as string) as website_url,
            cast(null as string) as linkedin_url,
            cast(null as string) as twitter_handle,
            cast(null as string) as facebook_url,
            cast(null as string) as instagram_handle,
            created_at,
            updated_at
        from candidates_pre
    ),

    deduplicated as (
        select *
        from candidates_with_id
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
