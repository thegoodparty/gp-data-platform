{{ config(materialized="view", schema="analytics") }}

/*
    analytics.analytics_users — v0.3: Onboarding CVR (DATA-1485)

    Unified user-grain view for Win and Serve product OKR metrics.
    Built incrementally: each version adds columns for a new metric.

    Grain: One row per user.
    Refresh: Full refresh (view).

    Note: File is named analytics_users.sql to avoid dbt model name collision
    with mart_civics users model.

    Version History:
    - v0.1: Registrations + product flags (is_win_user, is_serve_user) ✅
    - v0.2: Pro CVR — is_pro, pro_campaign_count ✅
    - v0.3: Onboarding CVR — is_onboarded + Amplitude milestone columns ✅
    - v0.4: (planned) 1st Campaign Sent — first_campaign_sent_at, is_activated
    - v0.5: (planned) Active Candidates — is_active, last_active_at

    v0.3 is_onboarded definition (authoritative KPI):
    - US registration (registration_country = 'United States')
    - Dashboard viewed within 14 days of Amplitude registration event
    - Standard metric filter: is_post_amplitude_registration = TRUE

    Supplemental note:
    - has_completed_onboarding_flow maps to the OKR doc alternative
      (onboarding_complete event only; no US or 14-day constraint).
*/
with

    users as (select * from {{ ref("goodparty_data_catalog", "users") }}),
    milestones as (select * from {{ ref("int__amplitude_user_milestones") }}),

    final as (
        select
            -- Primary key
            u.user_id,

            -- User profile
            u.email,
            u.first_name,
            u.last_name,
            u.phone,
            u.zip,

            -- Product flags
            u.has_campaign as is_win_user,
            u.is_serve_user,
            u.eo_activated_at,

            -- Campaign stats (from mart_civics.users)
            u.campaign_count,
            u.non_demo_campaign_count,
            u.verified_campaign_count,
            u.active_campaign_count,
            u.pro_campaign_count,
            u.pledged_campaign_count,
            u.first_campaign_created_at,
            u.last_campaign_created_at,
            u.has_verified_campaign,
            u.has_pledged_campaign,

            -- v0.1: Registrations (DATA-1484)
            u.created_at as registered_at,
            date_trunc('month', u.created_at) as registration_month,
            date_trunc('week', u.created_at) as registration_week,
            year(u.created_at) as registration_year,
            quarter(u.created_at) as registration_quarter,

            -- v0.2: Pro CVR (DATA-1486)
            (u.pro_campaign_count > 0) as is_pro,

            -- v0.3: Onboarding CVR (DATA-1485)
            -- Definition: Bryan Levine's Exec Dashboard KPI
            -- Registration Completed -> Dashboard Viewed within 14 days, US only
            m.amplitude_registration_completed_at,
            m.first_dashboard_viewed_at,
            m.registration_country,
            coalesce(
                m.registration_country = 'United States'
                and m.first_dashboard_viewed_at is not null
                and m.amplitude_registration_completed_at is not null
                and m.first_dashboard_viewed_at >= m.amplitude_registration_completed_at
                and m.first_dashboard_viewed_at
                <= m.amplitude_registration_completed_at + interval 14 days,
                false
            ) as is_onboarded,
            m.onboarding_completed_at,
            (m.onboarding_completed_at is not null) as has_completed_onboarding_flow,
            -- Current intermediate scope is registration-event users.
            (m.user_id is not null) as has_amplitude_data,
            (u.created_at >= '2023-12-10') as is_post_amplitude_registration

        -- v0.4: 1st Campaign Sent (DATA-1488) — placeholder
        -- first_campaign_sent_at,
        -- is_activated,
        -- total_campaigns_sent,
        -- v0.5: Active Candidates (DATA-1493) — placeholder
        -- is_active,
        -- last_active_at
        from users u
        left join milestones m on u.user_id = m.user_id
    )

select *
from final
