-- User-week model for Win product engagement (campaign sends + dashboard views).
-- Weekly analog of int__amplitude_win_activity. Weeks are UTC ISO-Monday-anchored,
-- so activity is bucketed by UTC week, not the user's local week.
with
    win_events as (
        select
            try_cast(user_id as bigint) as user_id,
            cast(date_trunc('week', event_time) as date) as week_start_date,
            event_type,
            event_time,
            coalesce(
                try_cast(event_properties:recipientcount as bigint),
                try_cast(event_properties:votercontacts as bigint)
            ) as raw_recipient_count,
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
            -- Recurrent-activity events come from the single-source catalog
            -- instead of a hardcoded list. Resolves to 3 events:
            -- 'Voter Outreach - Campaign Completed', plus both the legacy
            -- and live dashboard-view events (unioned below via
            -- is_dashboard_view_event).
            and event_type in (
                select event_type
                from {{ ref("int__amplitude_event_catalog") }}
                where is_recurrent
            )
    ),

    dashboard_view_flags as (
        select
            user_id,
            event_time,
            {{ dashboard_view_is_new("event_time", "user_id") }} as is_new_view
        from win_events
        where {{ is_dashboard_view_event("event_type") }}
    ),

    dashboard_views_dedup as (
        select
            user_id,
            cast(date_trunc('week', event_time) as date) as week_start_date,
            count_if(is_new_view) as dashboard_views
        from dashboard_view_flags
        group by 1, 2
    ),

    final as (
        select
            we.user_id,
            we.week_start_date,
            date_add(we.week_start_date, 6) as week_end_date,

            -- Campaign activity
            count(
                case when event_type = 'Voter Outreach - Campaign Completed' then 1 end
            ) as campaigns_sent,
            sum(
                case
                    when event_type = 'Voter Outreach - Campaign Completed'
                    then recipient_count
                end
            ) as recipient_count,
            min(
                case
                    when event_type = 'Voter Outreach - Campaign Completed'
                    then event_time
                end
            ) as first_campaign_sent_at,
            max(
                case
                    when event_type = 'Voter Outreach - Campaign Completed'
                    then event_time
                end
            ) as last_campaign_sent_at,

            -- Dashboard activity
            coalesce(max(dv.dashboard_views), 0) as dashboard_views,
            count(
                distinct case
                    when {{ is_dashboard_view_event("event_type") }}
                    then date(event_time)
                end
            ) as dashboard_view_days,
            min(
                case
                    when {{ is_dashboard_view_event("event_type") }} then event_time
                end
            ) as first_dashboard_viewed_at,
            max(
                case
                    when {{ is_dashboard_view_event("event_type") }} then event_time
                end
            ) as last_dashboard_viewed_at,

            -- Combined activity summary
            count(*) as total_events,
            count(distinct date(event_time)) as activity_days,
            min(event_time) as first_activity_at,
            max(event_time) as last_activity_at
        from win_events we
        left join
            dashboard_views_dedup dv
            on we.user_id = dv.user_id
            and we.week_start_date = dv.week_start_date
        group by we.user_id, we.week_start_date
    )

select *
from final
