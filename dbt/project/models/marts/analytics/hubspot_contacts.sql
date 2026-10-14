{{ config(materialized="view") }}

/*
    Passthrough exposure of stg_airbyte_source__hubspot_api_contacts into
    mart_analytics for Sigma BI POV consumption. See amplitude_events.sql
    header for view-materialization rationale.
*/
select
    c.*,
    -- Staging keeps a merged-away contact's last extract; filter on this to
    -- count contacts that still exist in HubSpot.
    m.merged_contact_id is not null as is_merged_away
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as c
left join {{ ref("int__hubspot_contact_merges") }} as m on m.merged_contact_id = c.id
