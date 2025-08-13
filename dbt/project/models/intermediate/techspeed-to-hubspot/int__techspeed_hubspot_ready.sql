{{ config(materialized="view", tags=["techspeed", "hubspot", "export"]) }}

with
    latest_candidates as (
        select *
        from (
            select *,
                row_number() over (
                    partition by candidate_uuid 
                    order by updated_at desc
                ) as rn
            from {{ ref("int__techspeed_candidates_processed") }}
            where final_exclusion_reason is null
        ) ranked
        where rn = 1
    )

select
    -- Simple column renaming to HubSpot format (keeping nulls as nulls)
    candidate_id_source as `Candidate ID Source`,
    first_name as `First Name`,
    last_name as `Last Name`,
    candidate_type as `Candidate Type`,
    party as `Party Affiliation`,
    email as `Email`,
    phone as `Phone Number`,
    candidate_id_tier as `Candidate ID Tier`,
    website_url as `Website URL`,
    linkedin_url as `LinkedIn URL`,
    instagram_handle as `Instagram Handle`,
    twitter_handle as `Twitter Handle`,
    facebook_url as `Facebook URL`,
    birth_date as `Birth Date`,
    street_address as `Street Address`,
    state as `State/Region`,
    postal_code as `Postal Code`,
    district as `District`,
    city as `City`,
    cast(population as string) as `Population`,
    official_office_name as `Official Office Name`,
    candidate_office as `Candidate Office`,
    standardized_office_type as `Office Type`,
    office_level as `Office Level`,
    filing_deadline as `Filing Deadline`,
    ballotready_race_id as `BallotReady Race ID`,
    cast(primary_election_date as string) as `Primary Election Date`,
    cast(corrected_general_election_date as string) as `General Election Date`,
    cast(election_date as string) as `Election Date`,
    election_type as `Election Type`,
    uncontested as `Uncontested`,
    cast(number_of_candidates as string) as `Number of Candidates`,
    cast(number_of_seats_available as string) as `Number of Seats Available`,
    open_seat as `Open Seat`,
    partisan as `Partisan Type`,
    type as `Type`,
    contact_owner as `Contact Owner`,
    owner_name as `Owner Name`,
    current_date() as `Upload Date`,
    created_at as `created_at`,
    updated_at as `updated_at`

from latest_candidates
