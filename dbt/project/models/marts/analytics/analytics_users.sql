{{ config(materialized="view", schema="analytics") }}

/*
    analytics.analytics_users

    Unified user-grain view for product metrics.
    Source: mart_civics.users + int__amplitude_user_milestones.

    Metric definitions in this model:
    - is_pro: user has at least one pro campaign.
    - is_onboarded: US registration with first dashboard view from registration
      time through 14 days. Use with is_post_amplitude_registration for
      Amplitude-era denominators.
    - is_activated: user has at least one voter outreach campaign event.
    - has_completed_onboarding_flow: supplemental onboarding_complete flag.
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

            -- Registration cohort fields
            u.created_at as registered_at,
            date_trunc('month', u.created_at) as registration_month,
            date_trunc('week', u.created_at) as registration_week,
            year(u.created_at) as registration_year,
            quarter(u.created_at) as registration_quarter,

            -- Pro metric
            (u.pro_campaign_count > 0) as is_pro,

            -- Onboarding metric
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
            -- Intermediate is currently scoped to registration-event users.
            (m.user_id is not null) as has_amplitude_data,
            (u.created_at >= '2023-12-10') as is_post_amplitude_registration,

            -- Activated metric
            m.first_campaign_sent_at,
            (m.first_campaign_sent_at is not null) as is_activated,
            m.total_campaigns_sent
        from users u
        left join milestones m on u.user_id = m.user_id
    )

select *
from final
