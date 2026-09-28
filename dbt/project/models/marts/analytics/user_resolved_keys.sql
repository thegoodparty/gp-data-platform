{{ config(materialized="view") }}

/*
    Exposure of int__user_resolved_keys for the journey wide table and Sigma.
    See amplitude_events.sql for the view-materialization rationale.
*/
select *
from {{ ref("int__user_resolved_keys") }}
