{{ config(materialized="view") }}

/*
    Exposure of int__user_hubspot_touches: the sales and marketing contact
    history behind the per-user touch counts, one row per user per engagement.
    See amplitude_events.sql for the view-materialization rationale.
*/
select *
from {{ ref("int__user_hubspot_touches") }}
