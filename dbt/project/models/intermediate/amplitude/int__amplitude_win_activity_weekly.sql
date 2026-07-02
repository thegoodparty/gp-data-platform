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
            -- instead of a hardcoded list. Resolves to the same 2 events:
            -- 'Voter Outreach - Campaign Completed',
            -- 'Dashboard - Candidate Dashboard Viewed'.
            and event_type in (
                select event_type
                from {{ ref("int__amplitude_event_catalog") }}
                where is_recurrent
            )
    ),

    final as (
        select
            user_id,
            week_start_date,
            date_add(week_start_date, 6) as week_end_date,

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
            count(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed' then 1
                end
            ) as dashboard_views,
            count(
                distinct case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed'
                    then date(event_time)
                end
            ) as dashboard_view_days,
            min(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed'
                    then event_time
                end
            ) as first_dashboard_viewed_at,
            max(
                case
                    when event_type = 'Dashboard - Candidate Dashboard Viewed'
                    then event_time
                end
            ) as last_dashboard_viewed_at,

            -- Combined activity summary
            count(*) as total_events,
            count(distinct date(event_time)) as activity_days,
            min(event_time) as first_activity_at,
            max(event_time) as last_activity_at
        from win_events
        group by 1, 2
    )

select *
from final
