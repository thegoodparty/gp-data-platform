{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- Product DB users -> Civics mart candidate schema.
-- Grain: one row per user with campaign_count > 0. Schema aligns with
-- int__civics_candidate_ballotready / _techspeed for the downstream union.
with
    latest_campaigns as (select * from {{ ref("campaigns") }} where is_latest_version),

    users_with_real_campaign as (
        -- 2026+ scope: matches int__civics_candidacy_gp_api's filter so the
        -- two int models cover the same universe of campaigns/users. Pre-2026
        -- users belong to int__civics_candidate_2025 (HubSpot archive).
        select distinct user_id
        from latest_campaigns
        where
            not coalesce(is_demo, false)
            and ballotready_position_id is not null
            and election_date >= '2026-01-01'
    ),

    users_filtered as (
        select
            u.user_id,
            u.first_name,
            u.last_name,
            u.email,
            u.phone,
            u.created_at,
            u.updated_at
        from {{ ref("users") }} as u
        inner join users_with_real_campaign as uw on u.user_id = uw.user_id
        where u.campaign_count > 0
    ),

    user_state as (
        select user_id, campaign_state as state
        from latest_campaigns
        where not is_demo
        qualify row_number() over (partition by user_id order by created_at desc) = 1
    ),

    -- max() across a user's campaigns: all stages of a campaign resolve to
    -- the same canonical candidate so any non-null wins.
    user_er_canonical as (
        select c.user_id, max(xw.canonical_gp_candidate_id) as canonical_gp_candidate_id
        from latest_campaigns as c
        inner join
            {{ ref("int__civics_er_canonical_ids") }} as xw
            on c.campaign_id = xw.gp_api_campaign_id
        group by c.user_id
    ),

    -- hubspotid lives in the user's meta_data JSON, not on the users mart.
    user_hubspot as (
        select id as user_id, meta_data:hubspotid::string as hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    -- candidate_id_tier: PD's campaign.tier is WIN/LOSE/TOSSUP, not the
    -- HubSpot viability tier — leave null until a real tier source lands.
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
                    partition by {{ generate_gp_api_gp_candidate_id() }}
                ),
                {{ generate_gp_api_gp_candidate_id() }}
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
