{{ config(materialized="view") }}

/*
    Exposure of int__user_product_area_activity_monthly: where each user spends
    their active days, by product area and month, for the User lookup page's
    peer comparison. See amplitude_events.sql for the view-materialization
    rationale.
*/
select *
from {{ ref("int__user_product_area_activity_monthly") }}
