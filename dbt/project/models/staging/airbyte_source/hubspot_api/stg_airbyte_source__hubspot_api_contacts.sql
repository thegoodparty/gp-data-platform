select
    -- identifiers
    id,
    companies,

    -- personal information
    properties_full_name as full_name,
    properties_firstname as first_name,
    properties_lastname as last_name,
    properties_birth_date as birth_date,
    properties_email as email,
    properties_phone as phone,
    properties_website as website,
    properties_instagram_handle as instagram_handle,
    properties_linkedin_url as linkedin_url,
    properties_twitterhandle as twitter_handle,
    properties_facebook_url as facebook_url,
    properties_address as address,

    -- candidate information
    properties_br_race_id as br_race_id,
    get_json_object(properties, '$.br_candidacy_id') as br_candidacy_id,
    get_json_object(properties, '$.br_position_id') as br_position_id,
    properties_candidate_id_source as candidate_id_source,
    properties_candidate_id_tier as candidate_id_tier,
    get_json_object(properties, '$.candidate_id_result') as candidate_id_result,
    get_json_object(properties, '$.candidate_type') as candidate_type,
    {{ cast_to_boolean("get_json_object(properties, '$.confirmed_candidate')") }}
    as is_confirmed_candidate,
    {{ cast_to_boolean("get_json_object(properties, '$.filed_candidate')") }}
    as is_filed_candidate,
    {{ cast_to_boolean("get_json_object(properties, '$.pro_candidate')") }}
    as is_pro_candidate,
    {{ cast_to_boolean("get_json_object(properties, '$.has_ever_been_pro')") }}
    as has_ever_been_pro,
    {{ cast_to_boolean("properties_pledge_status") }} as is_pledged,
    get_json_object(properties, '$.pledge_status') as pledge_status,
    {{ cast_to_boolean("get_json_object(properties, '$.email_pledge')") }}
    as has_email_pledge,
    {{ cast_to_boolean("get_json_object(properties, '$.verbal_pledge')") }}
    as has_verbal_pledge,
    {{ cast_to_boolean("get_json_object(properties, '$.verbal_pro')") }}
    as has_verbal_pro,
    {{ cast_to_boolean("get_json_object(properties, '$.verbal_serve')") }}
    as has_verbal_serve,
    properties_verified_candidate_status as verified_candidate_status,
    properties_type as type,
    properties_product_user as product_user,
    get_json_object(properties, '$.product_line') as product_line,
    get_json_object(properties, '$.product_stage') as product_stage,

    -- cross-system identifiers (sourced from HubSpot snapshot)
    -- NOTE: `goodparty_user_id` mirrors gp_api_db_user.id — the authoritative
    -- source is the users staging model from the gp_api database, not HubSpot.
    -- The HubSpot property is named after the goodparty.org domain (not an
    -- "org user" concept); it is the only user-ID key on the contact.
    cast(
        get_json_object(properties, '$.goodparty_org_user_id') as int
    ) as goodparty_user_id,

    -- office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_level as office_level,
    properties_office_type as office_type,
    properties_party_affiliation as party_affiliation,
    {{ cast_to_boolean("properties_partisan_type", ["partisan"], ["nonpartisan"]) }}
    as is_partisan,

    -- geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as candidate_district,
    {{ cast_to_boolean("properties_open_seat") }} as is_open_seat,
    cast(properties_population as int) as population,

    -- election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_election_date as primary_election_date,
    properties_election_date as election_date,
    properties_general_election_date as general_election_date,
    properties_start_date as start_date,

    -- election context
    {{ cast_to_boolean("properties_incumbent") }} as is_incumbent,
    {{ cast_to_boolean("properties_uncontested", ["uncontested"], ["contested"]) }}
    as is_uncontested,
    properties_number_opponents as number_opponents,
    cast(
        cast(properties_number_of_seats_available as float) as int
    ) as number_of_seats_available,

    -- hubspot ownership & marketing status
    get_json_object(properties, '$.hubspot_owner_id') as hubspot_owner_id,
    get_json_object(properties, '$.hs_marketable_status') as marketing_contact_status,
    {{ cast_to_boolean("get_json_object(properties, '$.hs_is_unworked')") }}
    as is_contact_unworked,

    -- hubspot lifecycle / journey
    get_json_object(properties, '$.lifecyclestage') as lifecycle_stage,
    get_json_object(properties, '$.hs_journey_stage') as journey_stage,
    get_json_object(properties, '$.hs_lead_status') as lead_status,
    get_json_object(properties, '$.lead_source') as lead_source,
    get_json_object(properties, '$.conversion_type') as conversion_type,

    -- hubspot lifecycle stage entry timestamps
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_current_stage') as timestamp
    ) as lifecycle_current_stage_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_customer') as timestamp
    ) as lifecycle_customer_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_evangelist') as timestamp
    ) as lifecycle_evangelist_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_lead') as timestamp
    ) as lifecycle_lead_entered_at,
    cast(
        get_json_object(
            properties, '$.hs_v2_date_entered_marketingqualifiedlead'
        ) as timestamp
    ) as lifecycle_mql_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_opportunity') as timestamp
    ) as lifecycle_opportunity_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_other') as timestamp
    ) as lifecycle_other_entered_at,
    cast(
        get_json_object(
            properties, '$.hs_v2_date_entered_salesqualifiedlead'
        ) as timestamp
    ) as lifecycle_sql_entered_at,
    cast(
        get_json_object(properties, '$.hs_v2_date_entered_subscriber') as timestamp
    ) as lifecycle_subscriber_entered_at,

    -- win pipeline stage and stage-entry timestamps
    get_json_object(properties, '$.win_stage') as win_stage,
    cast(
        get_json_object(properties, '$.date_entered_win_stage_0_intake') as timestamp
    ) as win_stage_0_intake_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_1_newassigned'
        ) as timestamp
    ) as win_stage_1_new_assigned_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_2_discovery_complete'
        ) as timestamp
    ) as win_stage_2_discovery_complete_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_3_pro_presented'
        ) as timestamp
    ) as win_stage_3_pro_presented_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_4_pro_consideration'
        ) as timestamp
    ) as win_stage_4_pro_consideration_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_5_closed_won__pro'
        ) as timestamp
    ) as win_stage_5_closed_won_pro_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_6_activated_pro_user'
        ) as timestamp
    ) as win_stage_6_activated_pro_user_entered_at,
    cast(
        get_json_object(
            properties, '$.date_entered_win_stage_closed_too_early'
        ) as timestamp
    ) as win_stage_closed_too_early_entered_at,

    -- win / serve ICP and activation
    -- NOTE: `win_icp` and `serve_icp` are stored in HubSpot as a snapshot of
    -- our own ICP model. The authoritative ICP values come from the ICP dbt
    -- model, not this staging column.
    get_json_object(properties, '$.win_icp') as win_icp,
    get_json_object(properties, '$.serve_icp') as serve_icp,
    -- NOTE: `win_activated_user` / `serve_activated_user` mirror activation
    -- status from the product `users` table; HubSpot holds a denormalized copy.
    get_json_object(properties, '$.win_activated_user') as win_activated_user,
    get_json_object(properties, '$.serve_activated_user') as serve_activated_user,

    -- serve pipeline stage, onboarding, and activity
    get_json_object(properties, '$.serve_stage') as serve_stage,
    get_json_object(properties, '$.serve_onboarding_stage') as serve_onboarding_stage,
    {{ cast_to_boolean("get_json_object(properties, '$.serve_opted_in')") }}
    as is_serve_opted_in,
    {{ cast_to_boolean("get_json_object(properties, '$.serve_results_viewed')") }}
    as has_serve_results_viewed,
    cast(
        get_json_object(properties, '$.serve_optin_date') as timestamp
    ) as serve_opt_in_at,
    cast(
        get_json_object(properties, '$.serve_activated_date') as timestamp
    ) as serve_activated_at,
    cast(
        get_json_object(properties, '$.serve_results_received_date') as timestamp
    ) as serve_results_received_at,
    cast(
        get_json_object(properties, '$.serve_results_viewed_date') as timestamp
    ) as serve_results_viewed_at,

    -- serve lifecycle stage entry timestamps
    cast(
        get_json_object(
            properties, '$.serve_lifecycle_stage__became_a_lead_date'
        ) as timestamp
    ) as serve_lifecycle_lead_entered_at,
    cast(
        get_json_object(
            properties, '$.servelifecyclestagebecameaconverteddate'
        ) as timestamp
    ) as serve_lifecycle_converted_entered_at,
    cast(
        get_json_object(
            properties, '$.servelifecyclestagebecameanactivateddate'
        ) as timestamp
    ) as serve_lifecycle_activated_entered_at,
    cast(
        get_json_object(
            properties, '$.serve_lifecycle_stage__became_an_noshow_sql_date'
        ) as timestamp
    ) as serve_lifecycle_noshow_sql_entered_at,
    cast(
        get_json_object(
            properties, '$.servelifecyclestagebecameanopportunitydate'
        ) as timestamp
    ) as serve_lifecycle_opportunity_entered_at,
    cast(
        get_json_object(properties, '$.servelifecyclestagebecameansaldate') as timestamp
    ) as serve_lifecycle_sal_entered_at,
    cast(
        get_json_object(properties, '$.servelifecyclestagebecameansqldate') as timestamp
    ) as serve_lifecycle_sql_entered_at,
    cast(
        get_json_object(
            properties, '$.servelifecyclestagebecamechurneddate'
        ) as timestamp
    ) as serve_lifecycle_churned_entered_at,
    cast(
        get_json_object(
            properties, '$.serve_lifecycle_stage_became_not_qualified_date'
        ) as timestamp
    ) as serve_lifecycle_not_qualified_entered_at,
    cast(
        get_json_object(
            properties, '$.servelifecyclestagebecamescheduledsqldate'
        ) as timestamp
    ) as serve_lifecycle_scheduled_sql_entered_at,
    cast(
        get_json_object(properties, '$.became_a_retained_date') as timestamp
    ) as serve_lifecycle_retained_entered_at,
    -- NOTE: "Serve Lifecycle Stages - Became an MQL date" has no matching
    -- HubSpot property key in current source data; column omitted.
    -- call and contact activity
    cast(get_json_object(properties, '$.last_call_date') as date) as last_call_at,
    get_json_object(properties, '$.last_call_outcome') as last_call_outcome,
    cast(
        get_json_object(properties, '$.notes_last_contacted') as timestamp
    ) as last_contacted_at,
    cast(
        get_json_object(properties, '$.number_of_serve_calls') as int
    ) as number_of_serve_calls,
    cast(
        get_json_object(properties, '$.pro_upgrade_date') as timestamp
    ) as pro_upgrade_at,

    -- metadata
    cast(
        get_json_object(properties, '$.createdate') as timestamp
    ) as contact_created_at,
    createdat as created_at,
    updatedat as updated_at
from {{ source("airbyte_source", "hubspot_api_contacts") }}
