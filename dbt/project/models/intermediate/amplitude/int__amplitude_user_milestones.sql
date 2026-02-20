{{ config(materialized="view") }}

/*
    int__amplitude_user_milestones

    Purpose:
        User-grain intermediate model that aggregates milestone events from Amplitude.
        Includes all users with at least one milestone event.
        Not scoped to registration-event users.
        Pre-bakes milestone columns used by downstream analytics marts.

    Grain:
        One row per user_id (BIGINT).

    Source:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}

    Event -> column mapping:
        - Onboarding - Registration Completed
            -> amplitude_registration_completed_at
            -> registration_country
        - Dashboard - Candidate Dashboard Viewed
            -> first_dashboard_viewed_at
            -> last_dashboard_viewed_at
            -> dashboard_view_count
        - onboarding_complete
            -> onboarding_completed_at
        - Voter Outreach - Campaign Completed
            -> first_campaign_sent_at
            -> total_campaigns_sent
            -> total_recipient_count
        - pro_upgrade_complete
            -> pro_upgrade_completed_at
        - Serve Onboarding - Getting Started Viewed
            -> serve_getting_started_at
        - Serve Onboarding - Constituency Profile Viewed
            -> serve_constituency_profile_at
        - Serve Onboarding - Poll Value Props Viewed
            -> serve_poll_value_props_at
        - Serve Onboarding - Poll Strategy Viewed
            -> serve_poll_strategy_at
        - Serve Onboarding - Add Image Viewed
            -> serve_add_image_at
        - Serve Onboarding - Poll Preview Viewed
            -> serve_poll_preview_at
        - Serve Onboarding - SMS Poll Sent
            -> first_sms_poll_sent_at

    Onboarding CVR definition note:
        Authoritative KPI is Registration Completed -> Dashboard Viewed within 14 days,
        US segment only. The 14-day and US logic is applied downstream in
        analytics_users, not in this intermediate model.

    Important:
        Event names are case-sensitive and must match Amplitude exactly.
*/
with
    milestone_events as (
        select
            try_cast(user_id as bigint) as user_id,
            event_type,
            event_time,
            country,
            coalesce(
                try_cast(event_properties:recipientcount as bigint),
                try_cast(event_properties:votercontacts as bigint)
            ) as raw_recipient_count,
            -- Data-quality guardrail: exclude implausible recipient counts caused by
            -- Amplitude instrumentation errors.
            case
                when raw_recipient_count > 100000
                then null
                when raw_recipient_count < 0
                then null
                else cast(raw_recipient_count as int)
            end as recipient_count
        from {{ ref("stg_airbyte_source__amplitude_api_events") }}
        where
            user_id is not null
            and try_cast(user_id as bigint) is not null
            and event_type in (
                'Onboarding - Registration Completed',
                'onboarding_complete',
                'pro_upgrade_complete',
                'Voter Outreach - Campaign Completed',
                'Dashboard - Candidate Dashboard Viewed',
                'Serve Onboarding - Getting Started Viewed',
                'Serve Onboarding - Constituency Profile Viewed',
                'Serve Onboarding - Poll Value Props Viewed',
                'Serve Onboarding - Poll Strategy Viewed',
                'Serve Onboarding - Add Image Viewed',
                'Serve Onboarding - Poll Preview Viewed',
                'Serve Onboarding - SMS Poll Sent'
            )
    ),

    final as (
        select
            user_id,

            -- Onboarding CVR
            min(
                case
                    when event_type = 'Onboarding - Registration Completed'
                    then event_time
                end
            ) as amplitude_registration_completed_at,
            min(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed'
                    then event_time
                end
            ) as first_dashboard_viewed_at,
            min_by(
                case
                    when event_type = 'Onboarding - Registration Completed' then country
                end,
                case
                    when event_type = 'Onboarding - Registration Completed'
                    then event_time
                end
            ) as registration_country,
            min(
                case when event_type = 'onboarding_complete' then event_time end
            ) as onboarding_completed_at,

            -- Activated Candidates
            min(
                case
                    when event_type = 'Voter Outreach - Campaign Completed'
                    then event_time
                end
            ) as first_campaign_sent_at,
            count(
                case when event_type = 'Voter Outreach - Campaign Completed' then 1 end
            ) as total_campaigns_sent,
            sum(
                case
                    when event_type = 'Voter Outreach - Campaign Completed'
                    then recipient_count
                end
            ) as total_recipient_count,

            -- Active Candidates
            max(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed'
                    then event_time
                end
            ) as last_dashboard_viewed_at,
            count(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed' then 1
                end
            ) as dashboard_view_count,

            -- Supplemental
            min(
                case when event_type = 'pro_upgrade_complete' then event_time end
            ) as pro_upgrade_completed_at,
            min(
                case
                    when event_type = 'Serve Onboarding - SMS Poll Sent' then event_time
                end
            ) as first_sms_poll_sent_at,

            -- Serve Onboarding Funnel (ordered by funnel step)
            min(
                case
                    when event_type = 'Serve Onboarding - Getting Started Viewed'
                    then event_time
                end
            ) as serve_getting_started_at,
            min(
                case
                    when event_type = 'Serve Onboarding - Constituency Profile Viewed'
                    then event_time
                end
            ) as serve_constituency_profile_at,
            min(
                case
                    when event_type = 'Serve Onboarding - Poll Value Props Viewed'
                    then event_time
                end
            ) as serve_poll_value_props_at,
            min(
                case
                    when event_type = 'Serve Onboarding - Poll Strategy Viewed'
                    then event_time
                end
            ) as serve_poll_strategy_at,
            min(
                case
                    when event_type = 'Serve Onboarding - Add Image Viewed'
                    then event_time
                end
            ) as serve_add_image_at,
            min(
                case
                    when event_type = 'Serve Onboarding - Poll Preview Viewed'
                    then event_time
                end
            ) as serve_poll_preview_at
        from milestone_events
        group by 1
    )

select *
from final
