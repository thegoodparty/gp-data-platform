{{ config(materialized="view") }}

/*
    mart_analytics.users_serve_base

    User-grain snapshot for Serve product metrics and the Serve onboarding funnel.
    Includes all users (not just Serve users) so downstream can compute
    conversion rates with correct denominators.

    Grain: One row per user_id.

    Source:
        mart_civics.users + int__amplitude_user_milestones + users_serve_activity.

    Metric columns:
        - Prospect Activation Rate: is_serve_user + eo_activated_at
        - Connect to Poll: has_sent_sms_poll
        - Active EOs: is_active_eo (trailing 30d)
        - Serve Onboarding Funnel: step completion timestamps + boolean flags
*/
with
    users as (select * from {{ ref("goodparty_data_catalog", "users") }}),
    milestones as (select * from {{ ref("int__amplitude_user_milestones") }}),

    -- Get most recent Serve activity timestamp per user for Active EO flag.
    serve_latest as (
        select user_id, max(last_activity_at) as last_serve_activity_at
        from {{ ref("users_serve_activity") }}
        group by 1
    ),

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

            -- Serve product flags
            -- Note: current civics data has is_serve_user and eo_activated_at aligned,
            -- so activation-rate denominator/numerator can be equivalent.
            u.is_serve_user,
            u.eo_activated_at,

            -- Active EO flag (AD-08: poll dashboard view in trailing 30d)
            s.last_serve_activity_at,
            coalesce(
                u.is_serve_user
                and s.last_serve_activity_at >= current_date - interval 30 days,
                false
            ) as is_active_eo,

            -- Connect to Poll (DATA-1499)
            m.first_sms_poll_sent_at,
            (m.first_sms_poll_sent_at is not null) as has_sent_sms_poll,

            -- Serve Onboarding Funnel timestamps
            m.serve_getting_started_at,
            m.serve_constituency_profile_at,
            m.serve_poll_value_props_at,
            m.serve_poll_strategy_at,
            m.serve_add_image_at,
            m.serve_poll_preview_at,

            -- Serve Onboarding Funnel boolean flags (for easy counting)
            (m.serve_getting_started_at is not null) as has_started_serve_onboarding,
            (
                m.serve_constituency_profile_at is not null
            ) as has_viewed_constituency_profile,
            (m.serve_poll_value_props_at is not null) as has_viewed_poll_value_props,
            (m.serve_poll_strategy_at is not null) as has_viewed_poll_strategy,
            (m.serve_add_image_at is not null) as has_added_image,
            (m.serve_poll_preview_at is not null) as has_previewed_poll,

            -- Funnel completion: reached the end of the onboarding flow.
            (m.first_sms_poll_sent_at is not null) as has_completed_serve_onboarding,

            -- Serve onboarding funnel step count (0-7 for dashboard segmentation)
            (
                case
                    when m.serve_getting_started_at is not null
                    then 1
                    else 0
                end + case
                    when m.serve_constituency_profile_at is not null then 1 else 0
                end
                + case when m.serve_poll_value_props_at is not null then 1 else 0 end
                + case when m.serve_poll_strategy_at is not null then 1 else 0 end
                + case when m.serve_add_image_at is not null then 1 else 0 end
                + case when m.serve_poll_preview_at is not null then 1 else 0 end
                + case when m.first_sms_poll_sent_at is not null then 1 else 0 end
            ) as serve_onboarding_steps_completed,

            -- Dashboard filter columns (AD-07: expose, don't hardcode)
            -- Note: is_verified/is_demo are campaign-grain in civics.
            -- Expose user-level analogs instead.
            coalesce(u.has_verified_campaign, false) as has_verified_campaign,

            -- Registration cohort fields
            u.created_at as registered_at,
            date_trunc('month', u.created_at) as registration_month,
            year(u.created_at) as registration_year,
            quarter(u.created_at) as registration_quarter

        from users u
        left join milestones m on u.user_id = m.user_id
        left join serve_latest s on u.user_id = s.user_id
    )

select *
from final
