{{
    config(
        materialized="table",
        tags=["techspeed", "hubspot", "export"],
        post_hook="insert into {{ ref('int__techspeed_uploaded_files') }} (source_file_url, uploaded_at, processing_date) select distinct _ab_source_file_url, current_timestamp(), current_date() from {{ this }} where final_exclusion_reason is null",
    )
}}

with
    deduplicated_candidates as (
        select * from {{ ref("int__techspeed_deduplicated_candidates") }}
    ),

    -- Clean district names
    district_cleaning as (
        select
            * except (district),
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                district, 'District |Dist\\. #?|Subdistrict |Ward ', ''
                            ),
                            '(st|nd|rd|th) Congressional District',
                            ''
                        ),
                        '[-#]',
                        ''
                    ),
                    ' District',
                    ''
                )
            ) as district
        from deduplicated_candidates
    ),

    -- Clean city names (remove everything after comma)
    city_cleaning as (
        select
            * except (city),
            case
                when city like '%,%'
                then left(city, position(',' in city) - 1)
                else city
            end as city
        from district_cleaning
    ),

    -- Final HubSpot formatting with null-to-empty-string conversion
    hubspot_formatted as (
        select
            -- Convert all nulls to empty strings for HubSpot compatibility
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

            -- Zero-pad postal codes to 5 digits
            case
                when postal_code is null
                then ''
                else right(concat('00000', cast(postal_code as string)), 5)
            end as `Postal Code`,

            case when district is null then '' else district end as `District`,
            case when city is null then '' else city end as `City`,
            case
                when population is null then '' else cast(population as string)
            end as `Population`,
            case
                when official_office_name is null then '' else official_office_name
            end as `Official Office Name`,
            case
                when candidate_office is null
                then ''
                else initcap(trim(candidate_office))
            end as `Candidate Office`,
            case
                when standardized_office_type is null
                then ''
                else standardized_office_type
            end as `Office Type`,
            case
                when office_level is null then '' else office_level
            end as `Office Level`,
            case
                when filing_deadline is null then '' else filing_deadline
            end as `Filing Deadline`,
            case
                when ballotready_race_id is null then '' else ballotready_race_id
            end as `BallotReady Race ID`,
            case
                when primary_election_date is null
                then ''
                else cast(primary_election_date as string)
            end as `Primary Election Date`,
            case
                when corrected_general_election_date is null
                then ''
                else cast(corrected_general_election_date as string)
            end as `General Election Date`,
            case
                when election_date is null then '' else cast(election_date as string)
            end as `Election Date`,
            case
                when election_type is null then '' else election_type
            end as `Election Type`,
            case when uncontested is null then '' else uncontested end as `Uncontested`,
            case
                when number_of_candidates is null
                then ''
                else cast(number_of_candidates as string)
            end as `Number of Candidates`,
            case
                when number_of_seats_available is null
                then ''
                else cast(number_of_seats_available as string)
            end as `Number of Seats Available`,
            case when open_seat is null then '' else open_seat end as `Open Seat`,
            case when partisan is null then '' else partisan end as `Partisan Type`,
            case when type is null then '' else type end as `Type`,
            case
                when contact_owner is null then '' else contact_owner
            end as `Contact Owner`,
            case when owner_name is null then '' else owner_name end as `Owner Name`,

            -- Keep exclusion reason and metadata for analysis
            final_exclusion_reason,
            current_date() as `Upload Date`,
            _ab_source_file_url

        from city_cleaning
    )

-- Return only candidates ready for upload (no exclusion reason)
select * except (_ab_source_file_url)
from hubspot_formatted
where final_exclusion_reason is null
