{{ config(materialized="view") }}

/*
    mart_analytics.users_serve_active

    Canonical gold-layer membership view for "active serve users" (epic
    DATA-1359): one row per user who completed Serve onboarding by sending an
    SMS poll AND has pledged. A thin projection of int__serve_active_user,
    which owns the definition — no logic lives here, so this mart cannot
    drift from the single source of truth.

    Exists so dashboards read the definition from a gold schema instead of
    re-deriving it from staging/intermediate tables (Sigma is losing access
    to those schemas, and business logic belongs in dbt, not dashboard SQL).

    Grain: one row per user_id, active serve users only — joining this model
    IS the active-user filter.

    Related surfaces: users_serve_base carries the component flags
    (has_sent_sms_poll, has_pledged, is_active_serve_user) over ALL users for
    funnel denominators. For the "empowered elected officials" dashboard
    cohort, join users_win_candidacy and apply icp_office_serve and the
    internal-email exclusion there.
*/
select user_id
from {{ ref("int__serve_active_user") }}
where is_active_serve_user
