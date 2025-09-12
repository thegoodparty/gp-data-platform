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
    techspeed_candidates_w_hubspot as (
        select
            -- We don't want to hand values over to hubspot with string
            -- values of 'null'. We just want the empty string
            case
                when candidate_id_source is null then '' else candidate_id_source
            end as `Candidate ID Source`,
            case when first_name is null then '' else first_name end as `First Name`,
            case when last_name is null then '' else last_name end as `Last Name`,
            case
                when candidate_type is null then '' else candidate_type
            end as `Candidate Type`,
            case when party is null then '' else party end as `Party Affiliation`,
            case when email is null then '' else email end as `Email`,
            case when phone is null then '' else phone end as `Phone Number`,
            case
                when candidate_id_tier is null then '' else candidate_id_tier
            end as `Candidate ID Tier`,
            case when website_url is null then '' else website_url end as `Website URL`,
            case
                when linkedin_url is null then '' else linkedin_url
            end as `LinkedIn URL`,
            case
                when instagram_handle is null then '' else instagram_handle
            end as `Instagram Handle`,
            case
                when twitter_handle is null then '' else twitter_handle
            end as `Twitter Handle`,
            case
                when facebook_url is null then '' else facebook_url
            end as `Facebook URL`,
            case when birth_date is null then '' else birth_date end as `Birth Date`,
            case
                when street_address is null then '' else street_address
            end as `Street Address`,
            case when state is null then '' else state end as `State/Region`,
            -- Postal codes must be zero-padded up to 5 digits
            case
                when postal_code is null
                then ''
                else right(concat('00000', cast(postal_code as varchar(10))), 5)
            end as postal_code,
            case when district is null then '' else district end as `District`,
            case when city is null then '' else city end as `City`,
            population,
            case
                when official_office_name is null then '' else official_office_name
            end as `Official Office Name`,
            case
                when candidate_office is null then '' else candidate_office
            end as `Candidate Office`,
            case when office_type is null then '' else office_type end as `Office Type`,
            case
                when office_level is null then '' else office_level
            end as `Office Level`,
            case
                when filing_deadline is null then '' else filing_deadline
            end as `Filing Deadline`,
            case
                when ballotready_race_id is null then '' else ballotready_race_id
            end as br_race_id,
            case
                when primary_election_date is null
                then ''
                else cast(primary_election_date as string)
            end as `Primary Election Date`,
            case
                when general_election_date is null
                then ''
                else cast(general_election_date as string)
            end as `General Election Date`,
            case
                when election_date is null then '' else cast(election_date as string)
            end as `Election Date`,
            case
                when election_type is null then '' else election_type
            end as `Election Type`,
            case when uncontested is null then '' else uncontested end as `Uncontested`,
            case
                when number_of_candidates is null then '' else number_of_candidates
            end as `Number of Candidates`,
            case
                when number_of_seats_available is null
                then ''
                else number_of_seats_available
            end as `Number of Seats Available`,
            case when open_seat is null then '' else open_seat end as `Open Seat`,
            case when partisan is null then '' else partisan end as `Partisan Type`,
            case when type is null then '' else type end as `Type`,
            case
                when contact_owner is null then '' else contact_owner
            end as `Contact Owner`,
            case when owner_name is null then '' else owner_name end as `Owner Name`,
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
            current_timestamp as added_to_mart_at,
            _ab_source_file_url
        from {{ ref("int__techspeed_candidates_fuzzy_deduped") }}
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
    added_to_mart_at
from techspeed_candidates_w_hubspot
