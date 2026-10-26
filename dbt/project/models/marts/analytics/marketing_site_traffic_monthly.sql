{{ config(materialized="view") }}

/*
    Exposure of int__marketing_site_traffic_monthly, so Sigma (which reads
    mart_analytics only) can draw signups per 1,000 site visitors.
    See amplitude_events.sql for the view-materialization rationale.
*/
select *
from {{ ref("int__marketing_site_traffic_monthly") }}
