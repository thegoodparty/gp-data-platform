{{
    config(
        materialized="incremental",
        unique_key="contact_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contacts", "hubspot"],
    )
}}

with
    extracted_engagements as (
        select distinct
            tbl_companies.id as company_id,
            regexp_extract(
                tbl_engagements.associations_companyids, '\\[(\\d+)\\]', 1
            ) as company_id_association,
            regexp_extract(
                tbl_engagements.associations_contactids, '\\[(\\d+)\\]', 1
            ) as contact_id_association
        from {{ ref("stg_airbyte_source__hubspot_api_companies") }} as tbl_companies
        left join
            {{ ref("stg_airbyte_source__hubspot_api_engagements") }} as tbl_engagements
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
                tbl_contacts.full_name, tbl_companies.properties_candidate_name
            ) as full_name,
            tbl_contacts.first_name as first_name,
            tbl_contacts.last_name as last_name,
            tbl_contacts.candidate_id_source as candidate_id_source,
            tbl_contacts.candidate_id_tier as candidate_id_tier,
            coalesce(
                tbl_contacts.email, tbl_companies.properties_candidate_email
            ) as email,
            coalesce(
                tbl_contacts.phone_number, tbl_companies.properties_phone
            ) as phone_number,
            coalesce(
                tbl_contacts.website_url, tbl_companies.properties_website
            ) as website_url,
            coalesce(
                tbl_contacts.linkedin_url,
                tbl_companies.properties_linkedin_company_page
            ) as linkedin_url,
            coalesce(
                tbl_contacts.twitter_handle, tbl_companies.properties_twitterhandle
            ) as twitter_handle,
            coalesce(
                tbl_contacts.facebook_url, tbl_companies.properties_facebook_url
            ) as facebook_url,
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
                tbl_contacts.party_affiliation, tbl_companies.properties_candidate_party
            ) as party_affiliation,
            coalesce(
                tbl_contacts.is_partisan,
                try_cast(tbl_companies.properties_partisan_np as string)
            ) as is_partisan,
            coalesce(
                tbl_contacts.state,
                tbl_states_company.state_cleaned_postal_code,
                tbl_companies.properties_state
            ) as state,
            coalesce(tbl_contacts.city, tbl_companies.properties_city) as city,
            coalesce(
                tbl_contacts.district, tbl_companies.properties_candidate_district
            ) as district,
            coalesce(
                tbl_contacts.seat,
                cast(tbl_companies.properties_candidates_seats as string)
            ) as seat,
            coalesce(
                tbl_contacts.filing_deadline, tbl_companies.properties_filing_deadline
            ) as filing_deadline,
            coalesce(
                tbl_contacts.primary_election_date,
                tbl_companies.properties_primary_date
            ) as primary_election_date,
            coalesce(
                tbl_contacts.general_election_date,
                tbl_companies.properties_election_date
            ) as general_election_date,
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
            tbl_contacts.birth_date as birth_date,
            tbl_contacts.instagram_handle as instagram_handle,
            tbl_contacts.population as population,
            tbl_contacts.email_contacts as email_contacts,
            tbl_companies.properties_open_seat_ as is_open_seat,
            tbl_companies.properties_general_election_result as candidacy_result,
            tbl_engagements.company_id_association,
            tbl_engagements.contact_id_association,

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

        from {{ ref("int__hubspot_contacts") }} as tbl_contacts
        left join
            extracted_engagements as tbl_engagements
            on tbl_contacts.id = tbl_engagements.contact_id_association
        left join
            {{ ref("stg_airbyte_source__hubspot_api_companies") }} as tbl_companies
            on tbl_companies.id = tbl_engagements.company_id_association
        left join
            {{ ref("clean_states") }} as tbl_states_company
            on trim(upper(tbl_companies.properties_state))
            = tbl_states_company.state_raw
        {% if is_incremental() %}
            where tbl_contacts.updated_at >= (select max(updated_at) from {{ this }})
        {% endif %}
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
    full_name,
    first_name,
    last_name,
    email,
    candidate_id_source,
    candidate_id_tier,
    phone_number,
    website_url,
    linkedin_url,
    twitter_handle,
    facebook_url,
    street_address,
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    party_affiliation,
    is_partisan,
    state,
    city,
    district,
    seat,
    is_open_seat,
    candidacy_result,
    filing_deadline,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    is_incumbent,
    is_uncontested,
    number_of_opponents,
    created_at,
    updated_at,
    birth_date,
    instagram_handle,
    population,
    email_contacts,
    company_id_association,
    contact_id_association,
    email_match,
    name_match
from ranked_matches
where 1 = 1 and row_rank = 1 and row_rank_gp_candidacy_id = 1
