{{ config(materialized="view") }}

/*
    Exposure of int__user_win_activity_weekly: the weeks each user was active in
    Win, for weekly retention views. See amplitude_events.sql for the
    view-materialization rationale.
*/
select *
from {{ ref("int__user_win_activity_weekly") }}
