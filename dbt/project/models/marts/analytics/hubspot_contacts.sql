{{ config(materialized="view") }}

/*
    Passthrough exposure of stg_airbyte_source__hubspot_api_contacts into
    mart_analytics for Sigma BI POV consumption. See amplitude_events.sql
    header for view-materialization rationale.
*/
select *
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
