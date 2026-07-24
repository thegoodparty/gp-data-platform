-- User-month Win engagement activity: campaign sends (with recipient counts)
-- and dashboard views, at monthly grain (one row per user_id x month).
-- No email/internal exclusion here (unlike Serve MAU's @goodparty.org filter).
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
            -- Recurrent-activity events come from the single-source taxonomy
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
            date_trunc('month', event_time) as activity_month_year,
            count_if(is_new_view) as dashboard_views
        from dashboard_view_flags
        group by 1, 2
    ),

    final as (
        select
            we.user_id,
            we.activity_month_year,

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

            count(*) as total_events,
            count(distinct date(event_time)) as activity_days,
            min(event_time) as first_activity_at,
            max(event_time) as last_activity_at
        from win_events we
        left join
            dashboard_views_dedup dv
            on we.user_id = dv.user_id
            and we.activity_month_year = dv.activity_month_year
        group by we.user_id, we.activity_month_year
    )

select *
from final
