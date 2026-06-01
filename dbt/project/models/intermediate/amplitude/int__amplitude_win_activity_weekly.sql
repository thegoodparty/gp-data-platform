{{ config(materialized="view") }}

/*
    int__amplitude_win_activity_weekly

    Purpose:
        User-week intermediate model for Win product engagement activity.
        Weekly analog of int__amplitude_win_activity (monthly). Captures
        campaign sends (with recipient counts) and dashboard views at
        weekly grain for retention modeling and cohort analysis.

    Grain:
        One row per user_id (BIGINT) x week_start_date.

    Source:
        {{ ref('stg_airbyte_source__amplitude_api_events') }}

    Week boundaries:
        date_trunc('week', event_time) → ISO week start (Monday).
        week_start_date is Monday; week_end_date is the following Sunday.
        This lets downstream point-in-time features use week_end_date
        directly as asof_date.

    Timezone semantics:
        event_time is in UTC; date_trunc('week', ...) is ISO-Monday-anchored.
        Activity at Sunday 23:30 UTC (which is Monday morning in many
        non-UTC locales) is bucketed into the week containing that Sunday,
        not the user's local Monday. Fine for retention modeling at weekly
        cadence; flagged here so reviewers don't second-guess edge-of-week
        boundaries.

    Event -> column mapping:
        - Voter Outreach - Campaign Completed
            -> campaigns_sent (count)
            -> recipient_count (sum, with COALESCE + 100k cap)
            -> first_campaign_sent_at, last_campaign_sent_at
        - Dashboard - Candidate Dashboard Viewed
            -> dashboard_views (count)
            -> dashboard_view_days (distinct days)
            -> first_dashboard_viewed_at, last_dashboard_viewed_at
*/
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
            and event_type in (
                'Voter Outreach - Campaign Completed',
                'Dashboard - Candidate Dashboard Viewed'
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
