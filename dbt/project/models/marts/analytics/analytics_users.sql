{{ config(materialized="view", schema="analytics") }}

/*
    analytics.analytics_users — v0.1: Registrations (DATA-1484)

    Unified user-grain view for Win and Serve product OKR metrics.
    Built incrementally: each version adds columns for a new metric.

    Grain: One row per user.
    Refresh: Full refresh (view).

    Note: File is named analytics_users.sql to avoid dbt model name collision
    with mart_civics users model.

    Version History:
    - v0.1: Registrations + product flags (is_win_user, is_serve_user)
    - v0.2: (planned) Onboarding CVR — onboarding_completed_at, is_onboarded
    - v0.3: (planned) Pro CVR — pro_upgraded_at, is_pro
    - v0.4: (planned) 1st Campaign Sent — first_campaign_sent_at, is_activated
    - v0.5: (planned) Active Candidates — is_active, last_active_at
*/
with

    users as (select * from {{ ref("goodparty_data_catalog", "users") }}),

    final as (
        select
            -- Primary key
            user_id,

            -- User profile
            email,
            first_name,
            last_name,
            phone,
            zip,

            -- Product flags
            has_campaign as is_win_user,
            is_serve_user,
            eo_activated_at,

            -- Campaign stats (from mart_civics.users)
            campaign_count,
            non_demo_campaign_count,
            verified_campaign_count,
            active_campaign_count,
            pro_campaign_count,
            pledged_campaign_count,
            first_campaign_created_at,
            last_campaign_created_at,
            has_verified_campaign,
            has_pledged_campaign,

            -- v0.1: Registrations (DATA-1484)
            created_at as registered_at,
            date_trunc('month', created_at) as registration_month,
            date_trunc('week', created_at) as registration_week,
            year(created_at) as registration_year,
            quarter(created_at) as registration_quarter

        -- v0.2: Onboarding CVR (DATA-1485) — placeholder
        -- onboarding_completed_at,
        -- is_onboarded,
        -- v0.3: Pro CVR (DATA-1486) — placeholder
        -- pro_upgraded_at,
        -- is_pro,
        -- v0.4: 1st Campaign Sent (DATA-1488) — placeholder
        -- first_campaign_sent_at,
        -- is_activated,
        -- total_campaigns_sent,
        -- v0.5: Active Candidates (DATA-1493) — placeholder
        -- is_active,
        -- last_active_at
        from users
    )

select *
from final
