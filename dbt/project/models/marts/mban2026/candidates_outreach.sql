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
    h.is_uncontested,
    h.is_incumbent,
    h.is_open_seat,
    h.number_of_opponents,
    h.is_partisan,
    h.seats_available,

    -- Outreach fields (may be null for HubSpot companies without outreach)
    co.id as outreach_id,
    co.campaignid as campaign_id,
    co.name as outreach_name,
    co.title as outreach_title,
    co.script,
    co.status as outreach_status,
    co.outreach_type,
    co.date as outreach_date,
    co.message,
    co.phone_list_id,
    co.audience_request,
    co.voter_file_filter_id,
    co.createdat as outreach_created_at,
    co.updatedat as outreach_updated_at,

    -- l2 fields
    l.name as l2_name,
    l.state as l2_state,
    l.l2_district_name,
    l.l2_district_type

from {{ ref("stg_airbyte_source__hubspot_api_companies") }} as h
left join
    {{ ref("clean_states") }} as tbl_states
    on trim(upper(h.state)) = tbl_states.state_raw
left join
    (
        select
            get_json_object(c.data, '$.hubspotId') as hubspot_id,
            o.id,
            o.campaignid,
            o.name,
            o.title,
            o.script,
            o.status,
            o.outreach_type,
            o.date,
            o.message,
            o.phone_list_id,
            o.audience_request,
            o.voter_file_filter_id,
            o.createdat,
            o.updatedat
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as c
        inner join
            {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
            on c.id = o.campaignid
            and o.outreach_type = 'text'
            and o.date >= '2023-01-01'
    ) as co
    on h.id = co.hubspot_id
left join
    (
        select distinct name, state, l2_district_name, l2_district_type
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
        where l2_district_type != 'NOT_MATCHED'
    ) as l
    on h.candidate_office is not null
    and h.candidate_office != ''
    and lower(trim(h.candidate_office)) = lower(trim(l.name))
    and tbl_states.state_cleaned_postal_code = l.state
