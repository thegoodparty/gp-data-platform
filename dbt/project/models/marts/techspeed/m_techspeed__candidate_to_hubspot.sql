/*
This model must be materialized as a view because it contains columns with spaces and special characters
in their names (e.g., "Candidate ID Source", "First Name", "Party Affiliation", etc.).

Database tables typically cannot have column names with spaces or special characters, but views can
handle these column aliases. This allows us to maintain the exact column naming requirements
for downstream systems (like HubSpot) while working within database constraints.

If this were materialized as a table, the column names would need to be converted to valid
database identifiers (e.g., snake_case), which would break the expected schema for HubSpot integration.
*/
{{ config(materialized="view") }}

with
    techspeed_candidates_fuzzy as (
        select * from {{ ref("int__techspeed_candidates_fuzzy_deduped") }}
    ),
    techspeed_viability as (
        select * from {{ ref("int__techspeed_viability_scoring") }}
    ),
    techspeed_candidates_w_hubspot as (
        select
            -- We don't want to hand values over to hubspot with string
            -- values of 'null'. We just want the empty string
            case
                when f.candidate_id_source is null then '' else f.candidate_id_source
            end as `Candidate ID Source`,
            case
                when f.first_name is null then '' else f.first_name
            end as `First Name`,
            case when f.last_name is null then '' else f.last_name end as `Last Name`,
            case
                when f.candidate_type is null then '' else f.candidate_type
            end as `Candidate Type`,
            case when f.party is null then '' else f.party end as `Party Affiliation`,
            case when f.email is null then '' else f.email end as `Email`,
            case when f.phone is null then '' else f.phone end as `Phone Number`,
            case
                when f.candidate_id_tier is null then '' else f.candidate_id_tier
            end as `Candidate ID Tier`,
            case
                when f.website_url is null then '' else f.website_url
            end as `Website URL`,
            case
                when f.linkedin_url is null then '' else f.linkedin_url
            end as `LinkedIn URL`,
            case
                when f.instagram_handle is null then '' else f.instagram_handle
            end as `Instagram Handle`,
            case
                when f.twitter_handle is null then '' else f.twitter_handle
            end as `Twitter Handle`,
            case
                when f.facebook_url is null then '' else f.facebook_url
            end as `Facebook URL`,
            case
                when f.birth_date is null then '' else f.birth_date
            end as `Birth Date`,
            case
                when f.street_address is null then '' else f.street_address
            end as `Street Address`,
            case when f.state is null then '' else f.state end as `State/Region`,
            -- Postal codes must be zero-padded up to 5 digits
            case
                when f.postal_code is null
                then ''
                else right(concat('00000', cast(f.postal_code as varchar(10))), 5)
            end as postal_code,
            case when f.district is null then '' else f.district end as `District`,
            case when f.city is null then '' else f.city end as `City`,
            f.population,
            case
                when f.official_office_name is null then '' else f.official_office_name
            end as `Official Office Name`,
            case
                when f.candidate_office is null then '' else f.candidate_office
            end as `Candidate Office`,
            case
                when f.office_type is null then '' else f.office_type
            end as `Office Type`,
            case
                when f.office_level is null then '' else f.office_level
            end as `Office Level`,
            case
                when f.filing_deadline is null then '' else f.filing_deadline
            end as `Filing Deadline`,
            case
                when f.ballotready_race_id is null then '' else f.ballotready_race_id
            end as br_race_id,
            case
                when f.primary_election_date is null
                then ''
                else cast(f.primary_election_date as string)
            end as `Primary Election Date`,
            case
                when f.general_election_date is null
                then ''
                else cast(f.general_election_date as string)
            end as `General Election Date`,
            case
                when f.election_date is null
                then ''
                else cast(f.election_date as string)
            end as `Election Date`,
            case
                when f.election_type is null then '' else f.election_type
            end as `Election Type`,
            case
                when f.uncontested is null then '' else f.uncontested
            end as `Uncontested`,
            case
                when f.number_of_candidates is null then '' else f.number_of_candidates
            end as `Number of Candidates`,
            case
                when f.number_of_seats_available is null
                then ''
                else f.number_of_seats_available
            end as `Number of Seats Available`,
            case when f.open_seat is null then '' else f.open_seat end as `Open Seat`,
            case when f.partisan is null then '' else f.partisan end as `Partisan Type`,
            case when f.type is null then '' else f.type end as `Type`,
            case
                when f.contact_owner is null then '' else f.contact_owner
            end as `Contact Owner`,
            case
                when f.owner_name is null then '' else f.owner_name
            end as `Owner Name`,
            f.fuzzy_results_techspeed_candidate_code,
            f.fuzzy_matched_hubspot_candidate_code,
            f.fuzzy_match_score,
            f.fuzzy_match_rank,
            f.fuzzy_matched_hubspot_contact_id,
            f.fuzzy_matched_first_name,
            f.fuzzy_matched_last_name,
            f.fuzzy_matched_state,
            f.fuzzy_matched_office_type,
            f.uploaded,
            f._airbyte_extracted_at,
            current_timestamp as added_to_mart_at,
            f._ab_source_file_url,
            -- Add viability scores
            v.viability_rating_2_0,
            v.score_viability_automated
        from techspeed_candidates_fuzzy as f
        left join
            techspeed_viability as v
            on f.techspeed_candidate_code = v.techspeed_candidate_code
        -- remove candidates that have already been found in ballotready
        where
            f.fuzzy_matched_hubspot_candidate_code not in (
                select fuzzy_matched_hubspot_candidate_code
                from {{ ref("m_ballotready_internal__records_sent_to_hubspot") }}
                where fuzzy_matched_hubspot_candidate_code is not null
            )
    )

select
    `Candidate ID Source`,
    `First Name`,
    `Last Name`,
    `Candidate Type`,
    `Party Affiliation`,
    `Email`,
    `Phone Number`,
    `Candidate ID Tier`,
    `Website URL`,
    `LinkedIn URL`,
    `Instagram Handle`,
    `Twitter Handle`,
    `Facebook URL`,
    `Birth Date`,
    `Street Address`,
    `State/Region`,
    postal_code,
    `District`,
    `City`,
    `population`,
    `Official Office Name`,
    `Candidate Office`,
    `Office Type`,
    `Office Level`,
    `Filing Deadline`,
    br_race_id,
    `Primary Election Date`,
    `General Election Date`,
    `Election Date`,
    `Election Type`,
    `Uncontested`,
    `Number of Candidates`,
    `Number of Seats Available`,
    `Open Seat`,
    `Partisan Type`,
    `Type`,
    `Contact Owner`,
    `Owner Name`,
    _ab_source_file_url,
    fuzzy_results_techspeed_candidate_code,
    fuzzy_matched_hubspot_candidate_code,
    fuzzy_match_score,
    fuzzy_match_rank,
    fuzzy_matched_hubspot_contact_id,
    fuzzy_matched_first_name,
    fuzzy_matched_last_name,
    fuzzy_matched_state,
    fuzzy_matched_office_type,
    uploaded,
    _airbyte_extracted_at,
    added_to_mart_at,
    viability_rating_2_0,
    score_viability_automated
from techspeed_candidates_w_hubspot
