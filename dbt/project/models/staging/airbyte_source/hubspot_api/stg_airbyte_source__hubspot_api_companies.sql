-- Reads Lakeflow's companies table, not the Airbyte source directly: the name's
-- "airbyte" now covers two producers. The frozen Airbyte table stays readable, and
-- the two columns Lakeflow does not carry come from there by id.
--
-- Live ids only, deliberately: company_viability_hubspot INNER-joins this model as
-- its proof that a Record ID still exists in HubSpot, so a company that is frozen
-- but no longer in HubSpot would emit a deleted Record ID into a CSV a human
-- imports, and the import would error.
--
-- Lakeflow hands every company property over as text, so the numeric and boolean
-- columns are cast here, matching the registry's own patterns: a double round-trip,
-- because float is 32-bit here and rounds integers above about 16.7 million (a
-- statewide vote total can exceed that), and because float-formatted values
-- ("1.0", "498.0000000000") null out under a plain try_cast to int.
with
    lakeflow_companies as (
        select
            cast(hs_object_id as string) as id,
            br_race_id,
            candidate_name,
            candidate_email,
            candidate_office,
            candidate_party,
            candidate_district,
            verified_candidates,
            pledge_status,
            incumbent,
            official_office_name,
            office_level,
            office_type,
            partisan_np,
            open_seat_,
            phone,
            website,
            linkedin_company_page,
            twitterhandle,
            facebook_url,
            address,
            state,
            city,
            filing_deadline,
            primary_date,
            election_date,
            runoff_date,
            uncontested,
            number_of_opponents,
            seats_available,
            general_election_result,
            general_votes_received,
            total_general_votes_cast,
            viability_2_0,
            cast(createdate as timestamp) as created_at,
            cast(hs_lastmodifieddate as timestamp) as updated_at
        from {{ source("lakeflow_source", "companies") }}
    ),

    -- The two columns Lakeflow has no source for. candidates_seats is read by
    -- int__hubspot_contacts_w_companies, which coalesces it into its seat column,
    -- so dropping it would move published values. contacts is the association
    -- array; no Lakeflow table carries associations, and nothing in dbt reads this
    -- model's copy, so the frozen value is kept rather than nulled.
    frozen_extras as (
        select
            cast(id as string) as id,
            cast(properties_candidates_seats as int) as candidates_seats,
            contacts
        from {{ source("airbyte_source", "hubspot_api_companies") }}
    ),

    joined as (
        select
            lakeflow_companies.*, frozen_extras.candidates_seats, frozen_extras.contacts
        from lakeflow_companies
        left join frozen_extras on lakeflow_companies.id = frozen_extras.id
    )

select
    -- identifiers
    id,
    contacts,

    -- candidate information
    br_race_id,
    candidate_name,
    candidate_email,
    candidate_office,
    candidate_party,
    candidate_district,
    candidates_seats,
    verified_candidates,
    {{ cast_to_boolean("pledge_status") }} as is_pledged,
    {{ cast_to_boolean("incumbent") }} as is_incumbent,

    -- office information
    official_office_name,
    office_level,
    office_type,
    {{ cast_to_boolean("partisan_np", ["partisan"], ["nonpartisan"]) }} as is_partisan,
    {{ cast_to_boolean("open_seat_") }} as is_open_seat,

    -- contact information
    phone,
    website,
    linkedin_company_page,
    twitterhandle as twitter_handle,
    facebook_url,
    address,

    -- geographic information
    state,
    city,

    -- election dates
    filing_deadline,
    primary_date,
    election_date,
    runoff_date,

    -- election context
    {{ cast_to_boolean("uncontested", ["uncontested"], ["contested"]) }}
    as is_uncontested,
    number_of_opponents,
    -- Double, not float: float is 32-bit here and rounds above about 16.7 million,
    -- which statewide vote totals can exceed. No current value is affected; this
    -- is preventive.
    try_cast(
        try_cast(nullif(trim(seats_available), '') as double) as int
    ) as seats_available,

    -- election results
    general_election_result,
    try_cast(
        try_cast(nullif(trim(general_votes_received), '') as double) as int
    ) as general_votes_received,
    try_cast(
        try_cast(nullif(trim(total_general_votes_cast), '') as double) as int
    ) as total_general_votes_cast,

    -- assessments
    try_cast(nullif(trim(viability_2_0), '') as decimal(38, 10)) as viability_score,

    -- metadata
    created_at,
    updated_at
from joined
-- Email only: a company phone can be a shared campaign line.
where {{ dsar_not_suppressed("candidate_email", "email") }}
