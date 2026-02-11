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

{#
    TechSpeed candidate_office values that map to ICP-qualifying normalized
    position types. Used for fallback ICP logic when no BallotReady position
    match exists. Values must be lowercase to match lower(trim()) comparison.
#}
{%- set icp_qualifying_ts_offices = [
    "city council",
    "city commission",
    "city commissioner",
    "alderman",
    "alderperson",
    "village board",
    "village council",
    "village trustee",
    "village councill",
    "village president",
    "municipal assembly",
    "metro council",
    "legislative council",
    "city legislative council",
    "mayor",
    "town council",
    "town board",
    "town commission",
    "town commissioner",
    "town trustee",
    "town board member",
    "township board",
    "town board chairperson",
    "state legislature",
    "county council",
    "county commission",
    "county commissioner",
    "county legislator",
    "board of supervisor",
    "board of supervisors",
    "board of commissioner",
    "county committee",
    "township supervisor",
    "town supervisor",
    "county supervisor",
    "county executive",
    "city council president",
    "city president",
    "town select board",
    "town selectmen",
    "town selectperson",
    "town selectboard",
    "town meeting representative",
    "city ward moderator",
    "town moderator",
] -%}

with
    techspeed_candidates_fuzzy as (
        select * from {{ ref("int__techspeed_candidates_fuzzy_deduped") }}
    ),
    techspeed_viability as (
        select * from {{ ref("int__techspeed_viability_scoring") }}
    ),
    br_race as (select * from {{ ref("stg_airbyte_source__ballotready_api_race") }}),
    icp_offices as (select * from {{ ref("int__icp_offices") }}),
    techspeed_candidates_w_hubspot as (
        select
            -- We don't want to hand values over to hubspot with string
            -- values of 'null'. We just want the empty string
            coalesce(f.candidate_id_source, '') as `Candidate ID Source`,
            coalesce(f.first_name, '') as `First Name`,
            coalesce(f.last_name, '') as `Last Name`,
            coalesce(f.candidate_type, '') as `Candidate Type`,
            coalesce(f.party, '') as `Party Affiliation`,
            coalesce(f.email, '') as `Email`,
            coalesce(f.phone, '') as `Phone Number`,
            coalesce(f.candidate_id_tier, '') as `Candidate ID Tier`,
            coalesce(f.website_url, '') as `Website URL`,
            coalesce(f.linkedin_url, '') as `LinkedIn URL`,
            coalesce(f.instagram_handle, '') as `Instagram Handle`,
            coalesce(f.twitter_handle, '') as `Twitter Handle`,
            coalesce(f.facebook_url, '') as `Facebook URL`,
            coalesce(f.birth_date, '') as `Birth Date`,
            coalesce(f.street_address, '') as `Street Address`,
            coalesce(f.state, '') as `State/Region`,
            -- Postal codes must be zero-padded up to 5 digits
            coalesce(
                right(concat('00000', cast(f.postal_code as varchar(10))), 5), ''
            ) as postal_code,
            coalesce(f.district, '') as `District`,
            coalesce(f.city, '') as `City`,
            f.population,
            coalesce(f.official_office_name, '') as `Official Office Name`,
            coalesce(f.candidate_office, '') as `Candidate Office`,
            coalesce(f.office_type, '') as `Office Type`,
            coalesce(f.office_level, '') as `Office Level`,
            coalesce(f.filing_deadline, '') as `Filing Deadline`,
            coalesce(f.ballotready_race_id, '') as br_race_id,
            coalesce(
                cast(f.primary_election_date as string), ''
            ) as `Primary Election Date`,
            coalesce(
                cast(f.general_election_date as string), ''
            ) as `General Election Date`,
            coalesce(cast(f.election_date as string), '') as `Election Date`,
            coalesce(f.election_type, '') as `Election Type`,
            coalesce(f.uncontested, '') as `Uncontested`,
            coalesce(f.number_of_candidates, '') as `Number of Candidates`,
            coalesce(f.number_of_seats_available, '') as `Number of Seats Available`,
            coalesce(f.open_seat, '') as `Open Seat`,
            coalesce(f.partisan, '') as `Partisan Type`,
            coalesce(f.type, '') as `Type`,
            coalesce(f.contact_owner, '') as `Contact Owner`,
            coalesce(f.owner_name, '') as `Owner Name`,
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
            v.score_viability_automated,
            -- ICP flags: use BallotReady ICP offices when available,
            -- fall back to candidate_office + population for net-new records.
            coalesce(
                icp.icp_office_win,
                f.population between 500 and 50000
                and lower(trim(f.candidate_office)) in (
                    {% for office in icp_qualifying_ts_offices %}
                        '{{ office }}'{{ ',' if not loop.last }}
                    {% endfor %}
                )
            ) as icp_win,
            coalesce(
                icp.icp_office_serve,
                f.population between 1000 and 100000
                and lower(trim(f.candidate_office)) in (
                    {% for office in icp_qualifying_ts_offices %}
                        '{{ office }}'{{ ',' if not loop.last }}
                    {% endfor %}
                )
            ) as icp_serve
        from techspeed_candidates_fuzzy as f
        left join
            techspeed_viability as v
            on f.techspeed_candidate_code = v.techspeed_candidate_code
        left join br_race r on try_cast(f.ballotready_race_id as int) = r.database_id
        left join icp_offices icp on r.position.databaseid = icp.br_database_position_id
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
    score_viability_automated,
    icp_win,
    icp_serve
from techspeed_candidates_w_hubspot
