{{ config(materialized="view") }}

/*
    int__amplitude_win_activity

    Purpose:
        User-month intermediate model for Win product engagement activity.
        Captures campaign sends (with recipient counts) and dashboard views
        at monthly grain for time-series reporting.

    Grain:
        One row per user_id (BIGINT) x activity_month_year.

    Source:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}

    Event -> column mapping:
        - Voter Outreach - Campaign Completed
            -> campaigns_sent (count)
            -> recipient_count (sum, with COALESCE + 100k cap)
            -> first_campaign_sent_at, last_campaign_sent_at
        - Dashboard - Candidate Dashboard Viewed
            -> dashboard_views (count)
            -> dashboard_view_days (distinct days)
            -> first_dashboard_viewed_at, last_dashboard_viewed_at

    Definition notes:
        - Casts Amplitude string user_id with TRY_CAST(user_id AS BIGINT).
        - Excludes records where BIGINT cast fails (non-numeric or empty IDs).
        - recipientCount uses COALESCE(recipientCount, voterContacts) with
          100k cap and <0 exclusion.
        - No email/internal exclusion (Win doesn't have the @goodparty.org
          filtering that Serve MAU uses).
*/
with
    win_events as (
        select
            try_cast(user_id as bigint) as user_id,
            date_trunc('month', event_time) as activity_month_year,
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
            and event_type in (
                'Voter Outreach - Campaign Completed',
                'Dashboard - Candidate Dashboard Viewed'
            )
    ),

    final as (
        select
            user_id,
            activity_month_year,

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
