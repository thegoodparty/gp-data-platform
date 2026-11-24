-- Reads Lakeflow's companies table, with the two columns Lakeflow does not carry
-- joined from the frozen Airbyte table by id.
--
-- Live ids only: company_viability_hubspot INNER-joins this model as its proof a
-- Record ID still exists, so a frozen-only company would put a deleted id into an
-- import CSV and the import would error.
--
-- Lakeflow hands every property over as text, so the numeric casts use a double
-- round-trip: float is 32-bit here and rounds above about 16.7 million, and
-- float-formatted values ("1.0") null out under a plain try_cast to int.
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

    -- The two columns Lakeflow has no source for. candidates_seats feeds a
    -- coalesce in int__hubspot_contacts_w_companies, so dropping it would move
    -- published values.
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
