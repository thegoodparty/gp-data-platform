{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contacts", "hubspot"],
    )
}}

select
    -- identifiers and relations
    tbl_hs_contacts.id,
    case
        when
            tbl_hs_contacts.companies is null
            or trim(tbl_hs_contacts.companies) = ''
            or trim(tbl_hs_contacts.companies) = '[]'
        then null
        else from_json(tbl_hs_contacts.companies, 'array<string>')
    end as companies,  -- type is array<string>

    -- Personal information
    tbl_hs_contacts.full_name,
    tbl_hs_contacts.first_name,
    tbl_hs_contacts.last_name,
    tbl_hs_contacts.birth_date,
    tbl_hs_contacts.email,
    tbl_hs_contacts.email as email_contacts,
    tbl_hs_contacts.phone as phone_number,
    tbl_hs_contacts.website as website_url,
    tbl_hs_contacts.instagram_handle,
    tbl_hs_contacts.linkedin_url,
    tbl_hs_contacts.twitter_handle,
    tbl_hs_contacts.facebook_url,
    tbl_hs_contacts.address as street_address,
    tbl_hs_contacts.candidate_id_source,
    tbl_hs_contacts.candidate_id_tier,
    tbl_hs_contacts.pledge_status,
    tbl_hs_contacts.verified_candidate_status,

    -- Office information
    tbl_hs_contacts.official_office_name,
    tbl_hs_contacts.candidate_office,
    tbl_hs_contacts.office_level,
    tbl_hs_contacts.office_type,
    tbl_hs_contacts.party_affiliation,
    tbl_hs_contacts.partisan_type as is_partisan,

    -- Geographic information
    coalesce(tbl_states.state_cleaned_postal_code, tbl_hs_contacts.state) as state,
    tbl_hs_contacts.city,
    tbl_hs_contacts.candidate_district as district,
    tbl_hs_contacts.open_seat as seat,
    tbl_hs_contacts.population,

    -- Election dates
    tbl_hs_contacts.filing_deadline,
    tbl_hs_contacts.primary_election_date,
    coalesce(
        tbl_hs_contacts.election_date, tbl_hs_contacts.general_election_date
    ) as general_election_date,
    cast(null as date) as runoff_election_date,

    -- Election context
    tbl_hs_contacts.incumbent as is_incumbent,
    tbl_hs_contacts.uncontested as is_uncontested,
    tbl_hs_contacts.number_opponents as number_of_opponents,

    -- Metadata
    tbl_hs_contacts.created_at,
    tbl_hs_contacts.updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }} tbl_hs_contacts
left join
    {{ ref("clean_states") }} as tbl_states
    on trim(upper(tbl_hs_contacts.state)) = tbl_states.state_raw
where
    1 = 1
    and tbl_hs_contacts.first_name is not null
    and tbl_hs_contacts.last_name is not null
