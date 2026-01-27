select
    -- HubSpot company fields (candidacy-level data)
    h.id as hubspot_id,
    h.properties_state as state,
    h.properties_viability_2_0 as viability_score,
    h.properties_candidate_office as candidate_office,
    h.properties_official_office_name as official_office_name,
    h.properties_office_level as office_level,
    h.properties_office_type as office_type,
    h.properties_election_date as election_date,
    h.properties_general_election_result as general_election_result,
    h.properties_general_votes_received as general_votes_received,
    h.properties_total_general_votes_cast as total_general_votes_cast,
    h.properties_uncontested as is_uncontested,

    -- Outreach fields (may be null for HubSpot companies without outreach)
    o.id as outreach_id,
    o.campaignid as campaign_id,
    o.name as outreach_name,
    o.title as outreach_title,
    o.script,
    o.status as outreach_status,
    o.outreach_type,
    o.date as outreach_date,
    o.message,
    o.phone_list_id,
    o.audience_request,
    o.voter_file_filter_id,
    o.createdat as outreach_created_at,
    o.updatedat as outreach_updated_at
from {{ ref("stg_airbyte_source__hubspot_api_companies") }} as h
left join
    {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as c
    on h.id = get_json_object(c.data, '$.hubspotId')
left join
    {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
    on c.id = o.campaignid
    and o.outreach_type = 'text'
    and o.date >= '2023-01-01'
