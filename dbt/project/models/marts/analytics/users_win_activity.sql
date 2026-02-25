{{ config(materialized="view") }}

/*
    mart_analytics.users_win_activity

    User x month activity grain for Win product.
    Each row = one user with Win engagement activity in one calendar month.
    Captures campaign sends (with outreach volume) and dashboard views.

    Grain: One row per user_id x activity_month_year.

    Source: int__amplitude_win_activity + mart_civics.users.
*/
with
    activity as (select * from {{ ref("int__amplitude_win_activity") }}),
    users as (select * from {{ ref("goodparty_data_catalog", "users") }}),

    final as (
        select
            -- Activity fields
            a.user_id,
            a.activity_month_year,

            -- Campaign activity
            a.campaigns_sent,
            a.recipient_count,
            a.first_campaign_sent_at,
            a.last_campaign_sent_at,

            -- Dashboard activity
            a.dashboard_views,
            a.dashboard_view_days,
            a.first_dashboard_viewed_at,
            a.last_dashboard_viewed_at,

            -- Combined activity summary
            a.total_events,
            a.activity_days,
            a.first_activity_at,
            a.last_activity_at,

            -- Civics enrichment (nullable when no civics match)
            u.has_campaign as is_win_user,
            u.is_serve_user,
            u.created_at as registered_at,
            date_trunc('month', u.created_at) as registration_month
        from activity a
        left join users u on a.user_id = u.user_id
    )

select *
from final
