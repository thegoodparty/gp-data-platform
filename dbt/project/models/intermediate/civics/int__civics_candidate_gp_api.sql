-- Product DB users -> Civics mart candidate schema.
-- Grain: one row per user with at least one valid candidacy. Schema aligns
-- with int__civics_candidate_ballotready / _techspeed for the downstream union.
with
    latest_campaigns as (
        -- Scope for the state rollup; membership comes from the candidacy model.
        select *
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and not coalesce(is_demo, false)
            and ballotready_position_id is not null
            and election_date >= '2026-01-01'
    ),

    -- No valid candidacy means not a candidate.
    users_with_valid_candidacy as (
        select distinct c.user_id
        from latest_campaigns as c
        inner join
            {{ ref("int__civics_candidacy_gp_api") }} as cy
            on c.campaign_id = cy.product_campaign_id
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
        inner join users_with_valid_candidacy as uw on u.user_id = uw.user_id
    ),

    user_state as (
        select user_id, campaign_state as state
        from latest_campaigns
        qualify row_number() over (partition by user_id order by created_at desc) = 1
    ),

    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    user_hubspot as (
        select id as user_id, hubspot_contact_id
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
    ),

    -- candidate_id_tier: PD's campaign.tier is WIN/LOSE/TOSSUP, not the
    -- HubSpot viability tier — leave null until a real tier source lands.
    candidates_with_id as (
        select
            -- Person id (every staged user is in the person universe; the
            -- coalesce is a full-refresh guard with identical single-member
            -- semantics). Must match int__civics_candidacy_gp_api.
            coalesce(
                p.gp_person_id,
                {{
                    generate_salted_uuid(
                        fields=["'gp_api'", "cast(u.user_id as string)"],
                        salt="person",
                    )
                }}
            ) as gp_candidate_id,

            uh.hubspot_contact_id,
            u.user_id as prod_db_user_id,
            cast(null as string) as candidate_id_tier,
            u.first_name,
            u.last_name,
            concat(u.first_name, ' ', u.last_name) as full_name,
            cast(null as date) as birth_date,
            us.state,
            u.email,
            u.phone as phone_number,
            cast(null as string) as street_address,
            cast(null as string) as website_url,
            cast(null as string) as linkedin_url,
            cast(null as string) as twitter_handle,
            cast(null as string) as facebook_url,
            cast(null as string) as instagram_handle,
            u.created_at,
            u.updated_at
        from users_filtered as u
        left join user_state as us on u.user_id = us.user_id
        left join user_hubspot as uh on u.user_id = uh.user_id
        left join
            person_ids as p on 'gp_api|' || cast(u.user_id as string) = p.record_key
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
