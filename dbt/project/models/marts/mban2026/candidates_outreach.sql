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
    o.updatedat as outreach_updated_at,

    --l2 fields
    l.name as l2name,
    l.state as l2_state,
    l.l2_district_name,
    l.l2_district_type

from {{ ref("stg_airbyte_source__hubspot_api_companies") }} as h
left join
    {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as c
    on h.id = get_json_object(c.data, '$.hubspotId')
left join
    {{ ref("stg_airbyte_source__gp_api_db_outreach") }} as o
    on c.id = o.campaignid
    and o.outreach_type = 'text'
    and o.date >= '2023-01-01'
left join
    (select distinct
            name,
            state,
            l2_district_name,
            l2_district_type
     from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
    ) as l
    -- Match to L2 based on a combination of candidate office and state, which
    -- requires converting HubSpot state full names to state postal code abbreviations
    on h.candidate_office is not null
    and h.candidate_office != ''
    and h.candidate_office = l.name
    and CASE INITCAP(h.state) 
        WHEN 'Alabama' THEN 'AL'                                                  
        WHEN 'Alaska' THEN 'AK'                                                     
        WHEN 'Arizona' THEN 'AZ'
        WHEN 'Arkansas' THEN 'AR'                                                   
        WHEN 'California' THEN 'CA'                                                 
        WHEN 'Colorado' THEN 'CO'
        WHEN 'Connecticut' THEN 'CT'                                                
        WHEN 'Delaware' THEN 'DE'
        WHEN 'Florida' THEN 'FL'                                                    
        WHEN 'Georgia' THEN 'GA'
        WHEN 'Hawaii' THEN 'HI'                                                     
        WHEN 'Idaho' THEN 'ID'
        WHEN 'Illinois' THEN 'IL'
        WHEN 'Indiana' THEN 'IN'                                                    
        WHEN 'Iowa' THEN 'IA'
        WHEN 'Kansas' THEN 'KS'                                                     
        WHEN 'Kentucky' THEN 'KY'
        WHEN 'Louisiana' THEN 'LA'                                                  
        WHEN 'Maine' THEN 'ME'
        WHEN 'Maryland' THEN 'MD'                                                   
        WHEN 'Massachusetts' THEN 'MA'
        WHEN 'Michigan' THEN 'MI'
        WHEN 'Minnesota' THEN 'MN'                                                  
        WHEN 'Mississippi' THEN 'MS'
        WHEN 'Missouri' THEN 'MO'                                                   
        WHEN 'Montana' THEN 'MT'
        WHEN 'Nebraska' THEN 'NE'                                                   
        WHEN 'Nevada' THEN 'NV'
        WHEN 'New Hampshire' THEN 'NH'                                              
        WHEN 'New Jersey' THEN 'NJ'
        WHEN 'New Mexico' THEN 'NM'                                                 
        WHEN 'New York' THEN 'NY'
        WHEN 'North Carolina' THEN 'NC'                                             
        WHEN 'North Dakota' THEN 'ND'
        WHEN 'Ohio' THEN 'OH'
        WHEN 'Oklahoma' THEN 'OK'                                                   
        WHEN 'Oregon' THEN 'OR'
        WHEN 'Pennsylvania' THEN 'PA'                                               
        WHEN 'Rhode Island' THEN 'RI'
        WHEN 'South Carolina' THEN 'SC'                                             
        WHEN 'South Dakota' THEN 'SD'
        WHEN 'Tennessee' THEN 'TN'                                                  
        WHEN 'Texas' THEN 'TX'
        WHEN 'Utah' THEN 'UT'                                                       
        WHEN 'Vermont' THEN 'VT'
        WHEN 'Virginia' THEN 'VA'                                                   
        WHEN 'Washington' THEN 'WA'
        WHEN 'West Virginia' THEN 'WV'
        WHEN 'Wisconsin' THEN 'WI'                                                  
        WHEN 'Wyoming' THEN 'WY'
        WHEN 'District Of Columbia' THEN 'DC'                                       
        ELSE h.state
    END = l.state
