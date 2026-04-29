{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contacts", "hubspot"],
    )
}}

with
    -- Many races map to one position (primary + general for the same office),
    -- so this is many-to-one and will not fan out the contacts.
    br_race as (
        select
            database_id as race_database_id,
            position.`databaseId` as position_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }}
    ),

    br_candidacy as (
        select database_id as candidacy_database_id, position_database_id
        from {{ ref("int__ballotready_candidacy") }}
    ),

    -- gp_api stores HubSpot's COMPANY id on the campaign (not the contact id),
    -- and HubSpot contacts carry an array of associated companies. This lookup
    -- recovers a BR position for HubSpot contacts whose own br_*_id properties
    -- are unset — common for product users (e.g. PMF respondents).
    campaigns_by_company as (
        select hubspot_id as company_id, ballotready_position_id
        from {{ ref("campaigns") }}
        where
            is_latest_version
            and hubspot_id is not null
            and ballotready_position_id is not null
        qualify row_number() over (partition by hubspot_id order by updated_at desc) = 1
    ),

    contact_company_pairs as (
        select
            contacts.id as contact_id,
            explode(from_json(contacts.companies, 'array<string>')) as company_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as contacts
        where
            contacts.companies is not null
            and trim(contacts.companies) not in ('', '[]')
    ),

    -- Pick one resolved position per contact so downstream joins stay 1:1.
    contact_position_via_campaign as (
        select contact_id, ballotready_position_id
        from contact_company_pairs
        inner join
            campaigns_by_company
            on contact_company_pairs.company_id = campaigns_by_company.company_id
        qualify
            row_number() over (partition by contact_id order by ballotready_position_id)
            = 1
    ),

    -- Resolve an effective BR position ID. Prefer the directly-stored
    -- br_position_id, then race ID (highest HubSpot-side coverage), candidacy
    -- ID, and finally a linked gp_api campaign via the HubSpot company array.
    tbl_hs_contacts as (
        select
            contacts.*,
            coalesce(
                contacts.br_position_id,
                br_race.position_database_id,
                br_candidacy.position_database_id,
                contact_position_via_campaign.ballotready_position_id
            ) as effective_br_position_id
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as contacts
        left join br_race on contacts.br_race_id = br_race.race_database_id
        left join
            br_candidacy
            on contacts.br_candidacy_id = br_candidacy.candidacy_database_id
        left join
            contact_position_via_campaign
            on contacts.id = contact_position_via_campaign.contact_id
    ),

    -- Join ICP and pre-compute the effective-date guard once so icp_win and
    -- icp_win_supersize stay DRY in the final SELECT. The guard mirrors
    -- m_ballotready_internal__records_sent_to_hubspot: positions with a
    -- win_effective_date only count for elections on/after that date.
    contacts_with_icp as (
        select
            tbl_hs_contacts.*,
            tbl_states.state_cleaned_postal_code,
            tbl_icp_offices.icp_office_win,
            tbl_icp_offices.icp_office_serve,
            tbl_icp_offices.icp_win_supersize,
            tbl_icp_offices.icp_win_effective_date is not null
            and (
                cast(
                    coalesce(
                        tbl_hs_contacts.election_date,
                        tbl_hs_contacts.general_election_date,
                        tbl_hs_contacts.primary_election_date
                    ) as date
                )
                is null
                or cast(
                    coalesce(
                        tbl_hs_contacts.election_date,
                        tbl_hs_contacts.general_election_date,
                        tbl_hs_contacts.primary_election_date
                    ) as date
                )
                < tbl_icp_offices.icp_win_effective_date
            ) as is_before_icp_win_effective_date
        from tbl_hs_contacts
        left join
            {{ ref("clean_states") }} as tbl_states
            on trim(upper(tbl_hs_contacts.state)) = tbl_states.state_raw
        left join
            {{ ref("int__icp_offices") }} as tbl_icp_offices
            on tbl_hs_contacts.effective_br_position_id
            = tbl_icp_offices.br_database_position_id
    )

select
    -- identifiers and relations
    id,
    br_position_id,
    br_candidacy_id,
    br_race_id,
    effective_br_position_id,
    case
        when companies is null or trim(companies) = '' or trim(companies) = '[]'
        then null
        else from_json(companies, 'array<string>')
    end as companies,  -- type is array<string>

    -- Personal information
    full_name,
    first_name,
    last_name,
    birth_date,
    email,
    email as email_contacts,
    phone as phone_number,
    website as website_url,
    instagram_handle,
    linkedin_url,
    twitter_handle,
    facebook_url,
    address as street_address,
    candidate_id_source,
    candidate_id_tier,
    is_pledged,
    verified_candidate_status,

    -- Office information
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    party_affiliation,
    is_partisan,

    -- Geographic information
    coalesce(state_cleaned_postal_code, state) as state,
    city,
    candidate_district as district,
    cast(null as string) as seat,
    is_open_seat,
    population,

    -- Election dates
    filing_deadline,
    primary_election_date,
    coalesce(election_date, general_election_date) as general_election_date,
    cast(null as date) as runoff_election_date,

    -- Election context
    is_incumbent,
    is_uncontested,
    number_opponents as number_of_opponents,

    -- ICP flags
    case
        when is_before_icp_win_effective_date then false else icp_office_win
    end as icp_win,
    icp_office_serve as icp_serve,
    case
        when is_before_icp_win_effective_date then false else icp_win_supersize
    end as icp_win_supersize,

    -- Metadata
    created_at,
    updated_at
from contacts_with_icp
where first_name is not null and last_name is not null
