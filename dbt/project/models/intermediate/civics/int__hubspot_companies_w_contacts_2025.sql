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

-- Archived HubSpot companies with contacts from 2025 snapshot
-- Uses companies as the base table and joins contacts via the companies.contacts field
-- Prioritizes company field values over contact field values
-- Includes contacts without associated companies
with
    -- Extract contact IDs from the companies.contacts JSON array field
    companies_with_contacts as (
        select
            tbl_companies.id as company_id,
            tbl_companies.*,
            -- Parse the contacts array and extract individual contact IDs
            explode(
                from_json(tbl_companies.contacts, 'array<string>')
            ) as contact_id_extracted
        from {{ ref("int__hubspot_companies_archive_2025") }} as tbl_companies
        where tbl_companies.contacts is not null and tbl_companies.contacts != '[]'
    ),

    -- Companies without contacts (will be included with null contact data)
    companies_without_contacts as (
        select
            tbl_companies.id as company_id,
            tbl_companies.*,
            null as contact_id_extracted
        from {{ ref("int__hubspot_companies_archive_2025") }} as tbl_companies
        where tbl_companies.contacts is null or tbl_companies.contacts = '[]'
    ),

    -- Join companies to contacts, ranking by updated_at for disambiguation
    companies_joined_contacts as (
        select
            cwc.company_id,
            tbl_contacts.id as contact_id,
            cwc.contact_id_extracted,
            {{
                generate_salted_uuid(
                    fields=[
                        "coalesce(tbl_contacts.first_name, '')",
                        "coalesce(tbl_contacts.last_name, '')",
                        "coalesce(cwc.properties_state, tbl_contacts.state, '')",
                        "coalesce(cwc.properties_candidate_party, tbl_contacts.party_affiliation, '')",
                        "coalesce(cwc.properties_candidate_office, tbl_contacts.candidate_office, '')",
                        "coalesce(cwc.properties_election_date, tbl_contacts.general_election_date)",
                        "coalesce(cwc.properties_candidate_district, tbl_contacts.district, '')",
                    ]
                )
            }}
            as gp_candidacy_id,
            -- Prioritize company fields, fallback to contacts, then GP DB
            coalesce(
                tbl_gp_db_campaign.data:name::string,
                cwc.properties_candidate_name,
                tbl_contacts.full_name
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
                cwc.properties_candidate_email,
                tbl_contacts.email
            ) as email,
            tbl_gp_db_campaign.details:email::string as email_gp_db,
            coalesce(cwc.properties_phone, tbl_contacts.phone_number) as phone_number,
            coalesce(
                tbl_gp_db_campaign.details:website::string,
                cwc.properties_website,
                tbl_contacts.website_url
            ) as website_url,
            tbl_gp_db_campaign.details:website::string as website_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:linkedin::string,
                cwc.properties_linkedin_company_page,
                tbl_contacts.linkedin_url
            ) as linkedin_url,
            tbl_gp_db_campaign.details:linkedin::string as linkedin_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:twitter::string,
                cwc.properties_twitterhandle,
                tbl_contacts.twitter_handle
            ) as twitter_handle,
            tbl_gp_db_campaign.details:twitter::string as twitter_handle_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:facebook::string,
                cwc.properties_facebook_url,
                tbl_contacts.facebook_url
            ) as facebook_url,
            tbl_gp_db_campaign.details:facebook::string as facebook_url_gp_db,
            coalesce(
                cwc.properties_address, tbl_contacts.street_address
            ) as street_address,
            coalesce(
                cwc.properties_official_office_name, tbl_contacts.official_office_name
            ) as official_office_name,
            coalesce(
                cwc.properties_candidate_office, tbl_contacts.candidate_office
            ) as candidate_office,
            coalesce(
                cwc.properties_office_level, tbl_contacts.office_level
            ) as office_level,
            coalesce(
                cwc.properties_office_type, tbl_contacts.office_type
            ) as office_type,
            coalesce(
                tbl_gp_db_campaign.details:party::string,
                cwc.properties_candidate_party,
                tbl_contacts.party_affiliation
            ) as party_affiliation,
            tbl_gp_db_campaign.details:party::string as party_affiliation_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:`partisanType`::string,
                try_cast(cwc.properties_partisan_np as string),
                tbl_contacts.is_partisan
            ) as is_partisan,
            tbl_gp_db_campaign.details:`partisanType`::string as is_partisan_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:state::string,
                tbl_states_company.state_cleaned_postal_code,
                cwc.properties_state,
                tbl_contacts.state
            ) as state,
            tbl_gp_db_campaign.details:state::string as state_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:city::string,
                cwc.properties_city,
                tbl_contacts.city
            ) as city,
            tbl_gp_db_campaign.details:city::string as city_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:district::string,
                cwc.properties_candidate_district,
                tbl_contacts.district
            ) as district,
            tbl_gp_db_campaign.details:district::string as district_gp_db,
            coalesce(
                cast(cwc.properties_candidates_seats as string), tbl_contacts.seat
            ) as seat,
            coalesce(
                tbl_gp_db_campaign.details:`filingDeadline`::string,
                cwc.properties_filing_deadline,
                tbl_contacts.filing_deadline
            ) as filing_deadline,
            tbl_gp_db_campaign.details:`filingDeadline`::string
            as filing_deadline_gp_db,
            -- Prioritize company archived data over contacts and GP DB
            coalesce(
                cwc.properties_primary_date,
                tbl_contacts.primary_election_date,
                try_cast(
                    tbl_gp_db_campaign.details:`primaryElectionDate`::string as date
                )
            ) as primary_election_date,
            tbl_gp_db_campaign.details:`primaryElectionDate`::string
            as primary_election_date_gp_db,
            -- Prioritize company archived data over contacts and GP DB
            coalesce(
                cwc.properties_election_date,
                tbl_contacts.general_election_date,
                try_cast(tbl_gp_db_campaign.details:`electionDate`::string as date)
            ) as general_election_date,
            tbl_gp_db_campaign.details:`electionDate`::string
            as general_election_date_gp_db,
            coalesce(
                cwc.properties_runoff_date, tbl_contacts.runoff_election_date
            ) as runoff_election_date,
            coalesce(
                try_cast(cwc.properties_incumbent as string), tbl_contacts.is_incumbent
            ) as is_incumbent,
            coalesce(
                try_cast(cwc.properties_uncontested as string),
                tbl_contacts.is_uncontested
            ) as is_uncontested,
            coalesce(
                cwc.properties_number_of_opponents, tbl_contacts.number_of_opponents
            ) as number_of_opponents,
            coalesce(cwc.updatedat, tbl_contacts.updated_at) as updated_at,
            coalesce(cwc.createdat, tbl_contacts.created_at) as created_at,
            coalesce(
                tbl_gp_db_campaign.details:dob::string,
                cast(tbl_contacts.birth_date as string)
            ) as birth_date,
            tbl_gp_db_campaign.details:dob::string as birth_date_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:instagram::string,
                tbl_contacts.instagram_handle
            ) as instagram_handle,
            tbl_gp_db_campaign.details:instagram::string as instagram_handle_gp_db,
            tbl_contacts.population as population,
            tbl_contacts.email as email_contacts,
            tbl_contacts.companies as extra_companies,
            cwc.properties_open_seat_ as is_open_seat,
            cwc.properties_general_election_result as candidacy_result,
            coalesce(
                cwc.properties_verified_candidates,
                tbl_contacts.verified_candidate_status
            ) as verified_candidate,
            lower(
                coalesce(cwc.properties_pledge_status, tbl_contacts.pledge_status)
            ) as pledge_status,
            tbl_gp_db_campaign.id as product_campaign_id,
            -- assessments
            tbl_gp_db_ptv.data:`winNumber`::string as win_number,
            null::string as win_number_model,
            -- Matching logic
            case
                when lower(tbl_contacts.email) = lower(cwc.properties_candidate_email)
                then 1
                else 0
            end as email_match,
            case
                when
                    lower(trim(cwc.properties_candidate_name)) = lower(
                        trim(
                            concat(tbl_contacts.first_name, ' ', tbl_contacts.last_name)
                        )
                    )
                then 1
                else 0
            end as name_match,
            -- Rank for selecting best contact when multiple exist
            row_number() over (
                partition by cwc.company_id
                order by tbl_contacts.updated_at desc nulls last
            ) as contact_rank
        from companies_with_contacts cwc
        left join
            {{ ref("int__hubspot_contacts_2025") }} as tbl_contacts
            on cwc.contact_id_extracted = tbl_contacts.id
        left join
            {{ ref("clean_states") }} as tbl_states_company
            on trim(upper(cwc.properties_state)) = tbl_states_company.state_raw
        left join
            {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as tbl_gp_db_campaign
            on cwc.company_id = tbl_gp_db_campaign.data:hubspotid::string
        left join
            {{ ref("stg_airbyte_source__gp_api_db_path_to_victory") }} as tbl_gp_db_ptv
            on tbl_gp_db_campaign.id = tbl_gp_db_ptv.campaign_id
    ),

    -- Companies without contacts - create rows with null contact data
    companies_no_contacts_joined as (
        select
            cwoc.company_id,
            null as contact_id,
            null as contact_id_extracted,
            {{
                generate_salted_uuid(
                    fields=[
                        "''",
                        "''",
                        "coalesce(cwoc.properties_state, '')",
                        "coalesce(cwoc.properties_candidate_party, '')",
                        "coalesce(cwoc.properties_candidate_office, '')",
                        "cwoc.properties_election_date",
                        "coalesce(cwoc.properties_candidate_district, '')",
                    ]
                )
            }} as gp_candidacy_id,
            coalesce(
                tbl_gp_db_campaign.data:name::string, cwoc.properties_candidate_name
            ) as full_name,
            tbl_gp_db_campaign.data:name::string as full_name_gp_db,
            tbl_gp_db_campaign.details:`firstName`::string as first_name,
            tbl_gp_db_campaign.details:`firstName`::string as first_name_gp_db,
            tbl_gp_db_campaign.details:`lastName`::string as last_name,
            tbl_gp_db_campaign.details:`lastName`::string as last_name_gp_db,
            null as candidate_id_source,
            null as candidate_id_tier,
            coalesce(
                tbl_gp_db_campaign.details:email::string,
                cwoc.properties_candidate_email
            ) as email,
            tbl_gp_db_campaign.details:email::string as email_gp_db,
            cwoc.properties_phone as phone_number,
            coalesce(
                tbl_gp_db_campaign.details:website::string, cwoc.properties_website
            ) as website_url,
            tbl_gp_db_campaign.details:website::string as website_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:linkedin::string,
                cwoc.properties_linkedin_company_page
            ) as linkedin_url,
            tbl_gp_db_campaign.details:linkedin::string as linkedin_url_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:twitter::string,
                cwoc.properties_twitterhandle
            ) as twitter_handle,
            tbl_gp_db_campaign.details:twitter::string as twitter_handle_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:facebook::string,
                cwoc.properties_facebook_url
            ) as facebook_url,
            tbl_gp_db_campaign.details:facebook::string as facebook_url_gp_db,
            cwoc.properties_address as street_address,
            cwoc.properties_official_office_name as official_office_name,
            cwoc.properties_candidate_office as candidate_office,
            cwoc.properties_office_level as office_level,
            cwoc.properties_office_type as office_type,
            coalesce(
                tbl_gp_db_campaign.details:party::string,
                cwoc.properties_candidate_party
            ) as party_affiliation,
            tbl_gp_db_campaign.details:party::string as party_affiliation_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:`partisanType`::string,
                try_cast(cwoc.properties_partisan_np as string)
            ) as is_partisan,
            tbl_gp_db_campaign.details:`partisanType`::string as is_partisan_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:state::string,
                tbl_states_company.state_cleaned_postal_code,
                cwoc.properties_state
            ) as state,
            tbl_gp_db_campaign.details:state::string as state_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:city::string, cwoc.properties_city
            ) as city,
            tbl_gp_db_campaign.details:city::string as city_gp_db,
            coalesce(
                tbl_gp_db_campaign.details:district::string,
                cwoc.properties_candidate_district
            ) as district,
            tbl_gp_db_campaign.details:district::string as district_gp_db,
            cast(cwoc.properties_candidates_seats as string) as seat,
            coalesce(
                tbl_gp_db_campaign.details:`filingDeadline`::string,
                cwoc.properties_filing_deadline
            ) as filing_deadline,
            tbl_gp_db_campaign.details:`filingDeadline`::string
            as filing_deadline_gp_db,
            coalesce(
                cwoc.properties_primary_date,
                try_cast(
                    tbl_gp_db_campaign.details:`primaryElectionDate`::string as date
                )
            ) as primary_election_date,
            tbl_gp_db_campaign.details:`primaryElectionDate`::string
            as primary_election_date_gp_db,
            coalesce(
                cwoc.properties_election_date,
                try_cast(tbl_gp_db_campaign.details:`electionDate`::string as date)
            ) as general_election_date,
            tbl_gp_db_campaign.details:`electionDate`::string
            as general_election_date_gp_db,
            cwoc.properties_runoff_date as runoff_election_date,
            try_cast(cwoc.properties_incumbent as string) as is_incumbent,
            try_cast(cwoc.properties_uncontested as string) as is_uncontested,
            cwoc.properties_number_of_opponents as number_of_opponents,
            cwoc.updatedat as updated_at,
            cwoc.createdat as created_at,
            tbl_gp_db_campaign.details:dob::string as birth_date,
            tbl_gp_db_campaign.details:dob::string as birth_date_gp_db,
            tbl_gp_db_campaign.details:instagram::string as instagram_handle,
            tbl_gp_db_campaign.details:instagram::string as instagram_handle_gp_db,
            null as population,
            null as email_contacts,
            null as extra_companies,
            cwoc.properties_open_seat_ as is_open_seat,
            cwoc.properties_general_election_result as candidacy_result,
            cwoc.properties_verified_candidates as verified_candidate,
            lower(cwoc.properties_pledge_status) as pledge_status,
            tbl_gp_db_campaign.id as product_campaign_id,
            tbl_gp_db_ptv.data:`winNumber`::string as win_number,
            null::string as win_number_model,
            0 as email_match,
            0 as name_match,
            1 as contact_rank
        from companies_without_contacts cwoc
        left join
            {{ ref("clean_states") }} as tbl_states_company
            on trim(upper(cwoc.properties_state)) = tbl_states_company.state_raw
        left join
            {{ ref("stg_airbyte_source__gp_api_db_campaign") }} as tbl_gp_db_campaign
            on cwoc.company_id = tbl_gp_db_campaign.data:hubspotid::string
        left join
            {{ ref("stg_airbyte_source__gp_api_db_path_to_victory") }} as tbl_gp_db_ptv
            on tbl_gp_db_campaign.id = tbl_gp_db_ptv.campaign_id
    ),

    -- Contacts without associated companies
    contacts_without_companies as (
        select
            null as company_id,
            tbl_contacts.id as contact_id,
            null as contact_id_extracted,
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
            tbl_contacts.full_name,
            null as full_name_gp_db,
            tbl_contacts.first_name,
            null as first_name_gp_db,
            tbl_contacts.last_name,
            null as last_name_gp_db,
            tbl_contacts.candidate_id_source,
            tbl_contacts.candidate_id_tier,
            tbl_contacts.email,
            null as email_gp_db,
            tbl_contacts.phone_number,
            tbl_contacts.website_url,
            null as website_url_gp_db,
            tbl_contacts.linkedin_url,
            null as linkedin_url_gp_db,
            tbl_contacts.twitter_handle,
            null as twitter_handle_gp_db,
            tbl_contacts.facebook_url,
            null as facebook_url_gp_db,
            tbl_contacts.street_address,
            tbl_contacts.official_office_name,
            tbl_contacts.candidate_office,
            tbl_contacts.office_level,
            tbl_contacts.office_type,
            tbl_contacts.party_affiliation,
            null as party_affiliation_gp_db,
            tbl_contacts.is_partisan,
            null as is_partisan_gp_db,
            tbl_contacts.state,
            null as state_gp_db,
            tbl_contacts.city,
            null as city_gp_db,
            tbl_contacts.district,
            null as district_gp_db,
            tbl_contacts.seat,
            tbl_contacts.filing_deadline,
            null as filing_deadline_gp_db,
            tbl_contacts.primary_election_date,
            null as primary_election_date_gp_db,
            tbl_contacts.general_election_date,
            null as general_election_date_gp_db,
            tbl_contacts.runoff_election_date,
            tbl_contacts.is_incumbent,
            tbl_contacts.is_uncontested,
            tbl_contacts.number_of_opponents,
            tbl_contacts.updated_at,
            tbl_contacts.created_at,
            cast(tbl_contacts.birth_date as string) as birth_date,
            null as birth_date_gp_db,
            tbl_contacts.instagram_handle,
            null as instagram_handle_gp_db,
            tbl_contacts.population,
            tbl_contacts.email as email_contacts,
            tbl_contacts.companies as extra_companies,
            null as is_open_seat,
            null as candidacy_result,
            tbl_contacts.verified_candidate_status as verified_candidate,
            lower(tbl_contacts.pledge_status) as pledge_status,
            null as product_campaign_id,
            null as win_number,
            null as win_number_model,
            0 as email_match,
            0 as name_match,
            1 as contact_rank
        from {{ ref("int__hubspot_contacts_2025") }} as tbl_contacts
        where tbl_contacts.companies is null or tbl_contacts.companies = '[]'
    ),

    -- Union all sources: companies with contacts, companies without, contacts without
    -- companies
    all_records as (
        select *
        from companies_joined_contacts
        where contact_rank = 1
        union all
        select *
        from companies_no_contacts_joined
        union all
        select *
        from contacts_without_companies
    ),

    -- Final deduplication by gp_candidacy_id
    ranked_final as (
        select
            *,
            row_number() over (
                partition by gp_candidacy_id order by updated_at desc
            ) as row_rank_gp_candidacy_id
        from all_records
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
    email_match,
    name_match,
    win_number,
    win_number_model,
    product_campaign_id
from ranked_final
where row_rank_gp_candidacy_id = 1
