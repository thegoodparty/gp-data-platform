{{
    config(
        materialized="view",
        tags=["intermediate", "candidacy", "companies", "hubspot", "contacts"],
    )
}}

with
    extracted_engagements as (
        select distinct
            companies.companies_id_main,
            regexp_extract(
                engagements.associations_companyids, '\\[(\\d+)\\]', 1
            ) as company_id_association,
            regexp_extract(
                engagements.associations_contactids, '\\[(\\d+)\\]', 1
            ) as contact_id_association
        from {{ ref("int__hs_companies_recent") }} as companies
        left join
            {{ ref("stg_airbyte_source__hubspot_api_engagements") }} as engagements
            on companies.companies_id_main
            = regexp_extract(engagements.associations_companyids, '\\[(\\d+)\\]', 1)
        where companies.companies_id_main is not null
    ),

    joined_data as (
        select
            companies.*,
            -- TODO: add uuid, uniquness and non-null test
            {{
                generate_salted_uuid(
                    fields=[
                        "contacts.properties_firstname",
                        "contacts.properties_lastname",
                        "companies.state",
                        "companies.candidate_office",
                        "companies.general_election_date",
                        "companies.district",
                    ]
                )
            }} as gp_candidacy_id,
            contacts.id as contact_id,
            contacts.properties_candidate_id_source as candidate_id_source,
            contacts.properties_firstname as first_name,
            contacts.properties_lastname as last_name,
            contacts.properties_birth_date as birth_date,
            contacts.properties_instagram_handle as instagram_handle,
            contacts.properties_population as population,
            contacts.properties_candidate_id_tier as candidate_id_tier,
            contacts.properties_email as email_contacts,
            contacts.updatedat as contact_updated_at,

            -- Matching logic
            case
                when lower(contacts.properties_email) = lower(companies.email)
                then 1
                else 0
            end as email_match,

            case
                when
                    lower(trim(companies.full_name)) = lower(
                        trim(
                            concat(
                                contacts.properties_firstname,
                                ' ',
                                contacts.properties_lastname
                            )
                        )
                    )
                then 1
                else 0
            end as name_match

        from {{ ref("int__hs_companies_recent") }} as companies
        left join
            extracted_engagements as engagements
            on companies.companies_id_main = engagements.companies_id_main
        left join
            {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as contacts
            on contacts.id = engagements.contact_id_association
    ),

    ranked_matches as (
        select
            *,
            row_number() over (
                partition by companies_id_main
                order by email_match desc, name_match desc, contact_updated_at desc
            ) as row_rank
        from joined_data
    )

select *
from ranked_matches
where row_rank = 1
