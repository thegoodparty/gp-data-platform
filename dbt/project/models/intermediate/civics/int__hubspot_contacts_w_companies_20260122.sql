{{
    config(
        materialized="table",
        tags=[
            "intermediate",
            "civics",
            "contacts",
            "companies",
            "hubspot",
            "archive",
        ],
    )
}}

-- Archived HubSpot contacts with companies from 2026-01-22 snapshot
-- This model uses archived staging data to ensure historical consistency
with
    -- Extract company associations from the archived engagements
    extracted_engagements as (
        select distinct
            tbl_companies.id as company_id,
            regexp_extract(
                tbl_engagements.associations_companyids, '\\[(\\d+)\\]', 1
            ) as company_id_association,
            regexp_extract(
                tbl_engagements.associations_contactids, '\\[(\\d+)\\]', 1
            ) as contact_id_association
        from {{ ref("stg_archives__hubspot_api_companies_20260122") }} as tbl_companies
        left join
            {{ ref("stg_archives__hubspot_api_engagements_20260122") }}
            as tbl_engagements
            on tbl_companies.id
            = regexp_extract(tbl_engagements.associations_companyids, '\\[(\\d+)\\]', 1)
        where tbl_companies.id is not null
    ),
    joined_data as (
        select
            tbl_contacts.id as contact_id,
            tbl_companies.id as company_id,
            {{
                generate_salted_uuid(
                    fields=[
                        "tbl_contacts.first_name",
                        "tbl_contacts.last_name",
                        "tbl_contacts.state",
                        "tbl_contacts.party_affiliation",
                        "tbl_contacts.candidate_office",
                        "tbl_contacts.general_election_date",
                        "tbl_contacts.district",
                    ]
                )
            }} as gp_candidacy_id,
            coalesce(
                tbl_gp_db_campaign.data:name::string,
                tbl_contacts.full_name,
                tbl_companies.properties_candidate_name
            ) as full_name,
            tbl_gp_db_campaign.data:name::string as full_name_gp_db,
            coalesce(
                tbl_contacts.first_name, tbl_gp_db_campaign.details:`firstName`::string
            ) as first_name,
            tbl_gp_db_campaign.details:`firstName`::string as first_name_gp_db,
            coalesce(
                tbl_contacts.last_name, tbl_gp_db_campaign.details:`lastName`::string
            ) as last_name,
            tbl_gp_db_campaign.details:`lastName`::string as last_name_gp_db,
            tbl_contacts.candidate_id_source as candidate_id_source,
            tbl_contacts.candidate_id_tier as candidate_id_tier,
            coalesce(
                tbl_gp_db_campaign.details:email::string,
                tbl_contacts.email,
                tbl_companies.properties_candidate_email
            ) as email,
            tbl_gp_db_campaign.details:email::string as email_gp_db,
            coalesce(
                tbl_contacts.phone_number, tbl_companies.properties_phone
            ) as phone_number,
            coalesce(
                tbl_gp_db_campaign.details:website::string,
                tbl_contacts.website_url,
                tbl_companies.properties_website
            ) as website_url,
            tbl_gp_db_campaign.details:website::string as website_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:linkedin::string,
                tbl_contacts.linkedin_url,
                tbl_companies.properties_linkedin_company_page
            ) as linkedin_url,
            tbl_gp_db_campaign.details:linkedin::string as linkedin_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:twitter::string,
                tbl_contacts.twitter_handle,
                tbl_companies.properties_twitterhandle
            ) as twitter_handle,
            tbl_gp_db_campaign.details:twitter::string as twitter_handle_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:facebook::string,
                tbl_contacts.facebook_url,
                tbl_companies.properties_facebook_url
            ) as facebook_url,
            tbl_gp_db_campaign.details:facebook::string as facebook_url_gp_db,
            coalesce(
                tbl_contacts.street_address, tbl_companies.properties_address
            ) as street_address,
            coalesce(
                tbl_contacts.official_office_name,
                tbl_companies.properties_official_office_name
            ) as official_office_name,
            coalesce(
                tbl_contacts.candidate_office, tbl_companies.properties_candidate_office
            ) as candidate_office,
            coalesce(
                tbl_contacts.office_level, tbl_companies.properties_office_level
            ) as office_level,
            coalesce(
                tbl_contacts.office_type, tbl_companies.properties_office_type
            ) as office_type,
            coalesce(
                tbl_gp_db_campaign.details:party::string,
                tbl_contacts.party_affiliation,
                tbl_companies.properties_candidate_party
            ) as party_affiliation,
            tbl_gp_db_campaign.details:party::string as party_affiliation_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:`partisanType`::string,
                tbl_contacts.is_partisan,
                try_cast(tbl_companies.properties_partisan_np as string)
            ) as is_partisan,
            tbl_gp_db_campaign.details:`partisanType`::string as is_partisan_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:state::string,
                tbl_contacts.state,
                tbl_states_company.state_cleaned_postal_code,
                tbl_companies.properties_state
            ) as state,
            tbl_gp_db_campaign.details:state::string as state_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:city::string,
                tbl_contacts.city,
                tbl_companies.properties_city
            ) as city,
            tbl_gp_db_campaign.details:city::string as city_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:district::string,
                tbl_contacts.district,
                tbl_companies.properties_candidate_district
            ) as district,
            tbl_gp_db_campaign.details:district::string as district_gp_db,
            coalesce(
                tbl_contacts.seat,
                cast(tbl_companies.properties_candidates_seats as string)
            ) as seat,
            coalesce(
                tbl_gp_db_campaign.details:`filingDeadline`::string,
                tbl_contacts.filing_deadline,
                tbl_companies.properties_filing_deadline
            ) as filing_deadline,
            tbl_gp_db_campaign.details:`filingDeadline`::string
            as filing_deadline_gp_db,
            coalesce(
                try_cast(
                    tbl_gp_db_campaign.details:`primaryElectionDate`::string as date
                ),
                tbl_contacts.primary_election_date,
                tbl_companies.properties_primary_date
            ) as primary_election_date,
            tbl_gp_db_campaign.details:`primaryElectionDate`::string
            as primary_election_date_gp_db,
            coalesce(
                try_cast(tbl_gp_db_campaign.details:`electionDate`::string as date),
                tbl_contacts.general_election_date,
                tbl_companies.properties_election_date
            ) as general_election_date,
            tbl_gp_db_campaign.details:`electionDate`::string
            as general_election_date_gp_db,
            coalesce(
                tbl_contacts.runoff_election_date, tbl_companies.properties_runoff_date
            ) as runoff_election_date,
            coalesce(
                tbl_contacts.is_incumbent,
                try_cast(tbl_companies.properties_incumbent as string)
            ) as is_incumbent,
            coalesce(
                tbl_contacts.is_uncontested,
                try_cast(tbl_companies.properties_uncontested as string)
            ) as is_uncontested,
            coalesce(
                tbl_contacts.number_of_opponents,
                tbl_companies.properties_number_of_opponents
            ) as number_of_opponents,
            tbl_contacts.created_at,
            tbl_contacts.updated_at,
            coalesce(
                tbl_gp_db_campaign.details:dob::string, tbl_contacts.birth_date
            ) as birth_date,
            tbl_gp_db_campaign.details:dob::string as birth_date_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:instagram::string,
                tbl_contacts.instagram_handle
            ) as instagram_handle,
            tbl_gp_db_campaign.details:instagram::string as instagram_handle_gp_db,
            tbl_contacts.population as population,
            tbl_contacts.email_contacts as email_contacts,
            tbl_contacts.companies as extra_companies,
            tbl_companies.properties_open_seat_ as is_open_seat,
            tbl_companies.properties_general_election_result as candidacy_result,
            coalesce(
                tbl_contacts.verified_candidate_status,
                tbl_companies.properties_verified_candidates
            ) as verified_candidate,
            coalesce(
                tbl_contacts.pledge_status, tbl_companies.properties_pledge_status
            ) as pledge_status,
            tbl_engagements.company_id_association,
            tbl_engagements.contact_id_association,
            tbl_gp_db_campaign.id as product_campaign_id,

            -- assessments
            tbl_gp_db_ptv.data:`winNumber`::string as win_number,
            null::string as win_number_model,

            -- Matching logic
            case
                when
                    lower(tbl_contacts.email)
                    = lower(tbl_companies.properties_candidate_email)
                then 1
                else 0
            end as email_match,

            case
                when
                    lower(trim(tbl_companies.properties_candidate_name)) = lower(
                        trim(
                            concat(tbl_contacts.first_name, ' ', tbl_contacts.last_name)
                        )
                    )
                then 1
                else 0
            end as name_match

        from {{ ref("int__hubspot_contacts_20260122") }} as tbl_contacts
        left join
            extracted_engagements as tbl_engagements
            on tbl_contacts.id = tbl_engagements.contact_id_association
        left join
            {{ ref("stg_archives__hubspot_api_companies_20260122") }} as tbl_companies
            on tbl_companies.id = tbl_engagements.company_id_association
        left join
            {{ ref("clean_states") }} as tbl_states_company
            on trim(upper(tbl_companies.properties_state))
            = tbl_states_company.state_raw
        left join
            {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as tbl_gp_db_campaign
            on tbl_companies.id = tbl_gp_db_campaign.data:hubspotid::string
        left join
            {{ ref("stg_airbyte_source__gp_api_db_path_to_victory") }} as tbl_gp_db_ptv
            on tbl_gp_db_campaign.id = tbl_gp_db_ptv.campaign_id
    ),
    ranked_matches as (
        select
            *,
            row_number() over (
                partition by contact_id
                order by email_match desc, name_match desc, updated_at desc
            ) as row_rank,
            row_number() over (
                partition by gp_candidacy_id order by updated_at desc
            ) as row_rank_gp_candidacy_id
        from joined_data
    )

select
    contact_id,
    company_id,
    gp_candidacy_id,
    extra_companies,
    full_name,
    full_name_gp_db,
    first_name,
    first_name_gp_db,
    last_name,
    last_name_gp_db,
    email,
    email_gp_db,
    candidate_id_source,
    candidate_id_tier,
    phone_number,
    website_url,
    website_url_gp_db,
    linkedin_url,
    linkedin_url_gp_db,
    twitter_handle,
    twitter_handle_gp_db,
    facebook_url,
    facebook_url_gp_db,
    street_address,
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    party_affiliation,
    party_affiliation_gp_db,
    is_partisan,
    is_partisan_gp_db,
    state,
    state_gp_db,
    city,
    city_gp_db,
    district,
    district_gp_db,
    seat,
    is_open_seat,
    candidacy_result,
    verified_candidate,
    pledge_status,
    filing_deadline,
    filing_deadline_gp_db,
    primary_election_date,
    primary_election_date_gp_db,
    general_election_date,
    general_election_date_gp_db,
    runoff_election_date,
    is_incumbent,
    is_uncontested,
    number_of_opponents,
    created_at,
    updated_at,
    birth_date,
    birth_date_gp_db,
    instagram_handle,
    instagram_handle_gp_db,
    population,
    email_contacts,
    company_id_association,
    contact_id_association,
    email_match,
    name_match,
    win_number,
    win_number_model,
    product_campaign_id
from ranked_matches
where 1 = 1 and row_rank = 1 and row_rank_gp_candidacy_id = 1
