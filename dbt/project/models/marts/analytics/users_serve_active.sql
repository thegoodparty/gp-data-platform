{{ config(materialized="view") }}

/*
    Gold-layer membership view for "active serve users": one row per user who
    completed Serve onboarding by sending an SMS poll AND has pledged. A thin
    projection of int__serve_active_user, which owns the definition, so this
    mart cannot drift from the single source of truth.

    Grain: one row per user_id, active serve users only — joining this model
    IS the active-user filter.
*/
select user_id
from {{ ref("int__serve_active_user") }}
where is_active_serve_user
