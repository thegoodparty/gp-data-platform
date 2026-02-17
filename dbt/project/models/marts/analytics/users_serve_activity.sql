{{ config(materialized="view") }}

/*
    mart_analytics.users_serve_activity

    User x month activity grain for Serve product.
    Each row = one user active in one calendar month per the canonical
    Serve MAU definition: Viewed /dashboard/polls with
    Serve Activated = true, excluding @goodparty.org.

    Grain: One row per user_id x activity_month.

    Source: int__amplitude_serve_activity + mart_civics.users.
*/
with
    activity as (select * from {{ ref("int__amplitude_serve_activity") }}),
    users as (select * from {{ ref("goodparty_data_catalog", "users") }}),

    final as (
        select
            -- Activity fields
            a.user_id,
            a.activity_month,
            a.email,
            a.first_activity_at,
            a.last_activity_at,
            a.poll_view_count,
            a.activity_days,

            -- Civics enrichment (nullable when no civics match)
            u.is_serve_user,
            u.eo_activated_at,
            u.created_at as registered_at,
            date_trunc('month', u.created_at) as registration_month

        -- Future: office_type, office_level, state
        -- (pending officeholder data ingestion into civics mart)
        from activity a
        left join users u on a.user_id = u.user_id
    )

select *
from final
