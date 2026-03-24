select
    -- HubSpot company fields (candidacy-level data)
    h.id as hubspot_id,
    h.state,
    h.viability_score,
    h.candidate_office,
    h.official_office_name,
    h.office_level,
    h.office_type,
    h.election_date,
    h.general_election_result,
    h.general_votes_received,
    h.total_general_votes_cast,
    h.uncontested as is_uncontested,
    h.incumbent as is_incumbent,
    h.open_seat as is_open_seat,
    h.number_of_opponents,
    h.partisan_np as partisan_type,
    h.seats_available,

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
