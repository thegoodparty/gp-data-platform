{{ config(materialized="view", tags=["intermediate", "ballotready"]) }}

-- First step in extracting of BallotReady data for updating to hubspot
-- This file reads new ballotReady data and applies relevant field transformations
-- Select the difference between the current BallotReady data and the previous
-- BallotReady data
with
    br_new as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where candidacy_updated_at > current_date - interval 1 month
    ),
    br_formatted as (
        select
            -- ensure that there are not duplicates *within* the new BallotReady
            -- candidacies
            distinct
            election_date,
            position_name as official_office_name,
            candidate_office,
            office_level_formatted as office_level,
            civicengine_tier as geographic_tier,
            number_of_seats as number_of_seats_available,
            election_type,
            case
                when party like '%Independent%'
                then 'Independent'
                when party like '%Nonpartisan%'
                then 'Nonpartisan'
                else ''
            end as party_affiliation,
            concat_ws(
                '|', transform(parsed_parties, x -> x:name::string)
            ) as party_list,
            candidate_first_name as first_name,
            candidate_middle_name as middle_name,
            candidate_last_name as last_name,
            state_name as state,
            trim(regexp_replace(candidate_phone, '[^0-9]', '')) as phone,
            candidate_email as email,
            city,
            case
                when position_name like '%- District %'
                then regexp_extract(position_name, '- District (.*)$')
                when position_name like '% - Ward %'
                then regexp_extract(position_name, ' - Ward (.*)$')
                when position_name like '% - Place %'
                then regexp_extract(position_name, ' - Place (.*)$')
                when position_name like '% - Branch %'
                then regexp_extract(position_name, ' - Branch (.*)$')
                when position_name like '% - Subdistrict %'
                then regexp_extract(position_name, ' - Subdistrict (.*)$')
                when position_name like '% - Zone %'
                then regexp_extract(position_name, ' - Zone (.*)$')
                else ''
            end as district,
            case
                when position_name like '% - Seat %'
                then regexp_extract(position_name, ' - Seat ([^,]+)')
                when position_name like '% - Group %'
                then regexp_extract(position_name, ' - Group ([^,]+)')
                when position_name like '%, Seat %'
                then regexp_extract(position_name, ', Seat ([^,]+)')
                when position_name like '%: % - Seat %'
                then regexp_extract(position_name, ' - Seat ([^,]+)')
                when position_name like '% - Position %'
                then regexp_extract(position_name, ' - Position ([^\\s(]+)')
                else ''
            end as seat,
            {{
                generate_candidate_slug(
                    "candidate_first_name", "candidate_last_name", "position_name"
                )
            }} as candidate_slug,
            {{ map_ballotready_office_type("candidate_office") }} as office_type,
            'Self-Filer Lead' as type,
            'jesse@goodparty.org' as contact_owner,
            'Jesse Diliberto' as owner_name,
            'Ballotready' as candidate_id_source,
            _airbyte_raw_id,
            case
                when position_name is null
                then null
                when election_date is null
                then null
                when
                    regexp_replace(
                        regexp_replace(trim(position_name), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    )
                    = ''
                then null
                when
                    regexp_replace(
                        regexp_replace(trim(election_date), ' ', '-'),
                        '[^a-zA-Z0-9-]',
                        ''
                    )
                    = ''
                then null
                else
                    lower(
                        concat_ws(
                            '__',
                            regexp_replace(
                                regexp_replace(trim(position_name), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            ),
                            regexp_replace(
                                regexp_replace(trim(election_date), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            )
                        )
                    )
            end as br_contest_id,
            ballotready_candidacy_id as candidacy_id,
            ballotready_race_id,
            raw_parties as parties,
            candidacy_created_at,
            candidacy_updated_at
        from br_new
        -- what follows is the core substance of who is being selected for uploading
        -- to HubSpot
        where
            -- separate the records with both phones and emails
            candidate_phone is not null
            and candidate_email is not null
            -- remove any major party candidates
            and not is_major_party
            -- subset to candidates with future elections
            and election_date > current_date
    ),
    br_withcodes as (
        select
            *,
            {{
                generate_candidate_code(
                    "first_name",
                    "last_name",
                    "state",
                    "office_type",
                    "city",
                )
            }} as br_candidate_code
        from br_formatted
    )
select *
from br_withcodes
