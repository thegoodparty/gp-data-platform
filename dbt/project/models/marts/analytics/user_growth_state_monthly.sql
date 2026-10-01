{{ config(materialized="view") }}

/*
    Exposure of int__user_activity_monthly: each user's growth state month by
    month, for the cohort view. See amplitude_events.sql for the
    view-materialization rationale.
*/
select *
from {{ ref("int__user_activity_monthly") }}
