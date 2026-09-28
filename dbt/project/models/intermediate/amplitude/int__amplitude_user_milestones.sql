-- User-grain aggregation of Amplitude milestone events. Includes every user
-- with at least one milestone event, not just registration-event users.
-- Grain: one row per user_id.
-- Event names are case-sensitive and must match Amplitude exactly.
with
    milestone_events as (
        select
            try_cast(user_id as bigint) as user_id,
            event_type,
            event_time,
            country,
            event_properties:path::string as page_path,
            -- Distinguishes the three moments that share the Campaign Completed
            -- name; see is_outreach_activation_event.
            event_properties:method::string as outreach_method,
            -- Dedup key for the shared send terminal, which is once per committed
            -- payment rather than once per outreach.
            event_properties:outreachid::string as outreach_id,
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
            -- This is a bespoke milestone pick (specific named lifecycle events),
            -- not a taxonomy family, so the list stays explicit here. The
            -- single-source classifier is int__amplitude_event_catalog; a singular
            -- test (assert_milestone_events_classified) guards that every event
            -- this filter admits classifies to a non-'other' family.
            and (
                event_type in (
                    'Onboarding - Registration Completed',
                    'onboarding_complete',
                    'pro_upgrade_complete',
                    'Serve Onboarding - Getting Started Viewed',
                    'Serve Onboarding - Constituency Profile Viewed',
                    'Serve Onboarding - Poll Value Props Viewed',
                    'Serve Onboarding - Poll Strategy Viewed',
                    'Serve Onboarding - Add Image Viewed',
                    'Serve Onboarding - Poll Preview Viewed',
                    'Serve Onboarding - SMS Poll Sent'
                )
                -- The dashboard-view union is read from the same macro the consumers
                -- below use, so a re-anchor cannot admit rows downstream that this
                -- filter has already dropped. It has to be applied here and not only
                -- downstream: 'Viewed' is site-wide (4.5M rows) and only its ~106k
                -- '/dashboard' rows are dashboard views. The page_path select alias
                -- is not in scope in a WHERE clause, hence the raw expression.
                or {{
                    is_dashboard_view_event(
                        "event_type", "event_properties:path::string"
                    )
                }}
                -- Outreach terminals, read from their metric's declaration for the
                -- same reason. Select aliases are not in scope in a WHERE clause.
                or {{
                    is_outreach_activation_event(
                        "event_type", "event_properties:method::string"
                    )
                }}
            )
    ),

    send_commits as (
        select
            *,
            -- A lapsed robocall hold can be re-authorized and commits a second time
            -- under the same outreach id, days later and past Segment's dedup
            -- window. That is one send retried, so only the first commit counts.
            -- Legs that carry no outreach id cannot re-fire this way and always
            -- count; they must not be collapsed into a single null partition.
            case
                when outreach_id is null
                then true
                else
                    row_number() over (
                        partition by user_id, outreach_id order by event_time
                    )
                    = 1
            end as is_first_commit
        from milestone_events
    ),

    dashboard_view_flags as (
        select
            user_id, {{ dashboard_view_is_new("event_time", "user_id") }} as is_new_view
        from milestone_events
        where {{ is_dashboard_view_event("event_type", "page_path") }}
    ),

    dashboard_views_dedup as (
        select user_id, count_if(is_new_view) as dashboard_view_count
        from dashboard_view_flags
        group by 1
    ),

    final as (
        select
            me.user_id,

            -- Onboarding CVR
            min(
                case
                    when event_type = 'Onboarding - Registration Completed'
                    then event_time
                end
            ) as amplitude_registration_completed_at,
            min(
                case
                    when {{ is_dashboard_view_event("event_type", "page_path") }}
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
                    when
                        {{
                            is_outreach_activation_event(
                                "event_type", "outreach_method"
                            )
                        }} and is_first_commit
                    then event_time
                end
            ) as first_campaign_sent_at,
            count(
                case
                    when
                        {{
                            is_outreach_activation_event(
                                "event_type", "outreach_method"
                            )
                        }} and is_first_commit
                    then 1
                end
            ) as total_campaigns_sent,
            sum(
                case
                    when
                        {{
                            is_outreach_activation_event(
                                "event_type", "outreach_method"
                            )
                        }} and is_first_commit
                    then recipient_count
                end
            ) as total_recipient_count,

            -- Active Candidates
            max(
                case
                    when {{ is_dashboard_view_event("event_type", "page_path") }}
                    then event_time
                end
            ) as last_dashboard_viewed_at,
            coalesce(max(dv.dashboard_view_count), 0) as dashboard_view_count,

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
        from send_commits me
        left join dashboard_views_dedup dv on me.user_id = dv.user_id
        group by me.user_id
    )

select *
from final
