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
            election_day as election_date,
            position_name as official_office_name,
            {{
                generate_candidate_office_from_position(
                    "position_name", "normalized_position_name"
                )
            }} as candidate_office,
            case
                when level = 'local'
                then 'Local'
                when level = 'city'
                then 'City'
                when level = 'county'
                then 'County'
                when level = 'state'
                then 'State'
                else level
            end as office_level,
            tier as geographic_tier,
            number_of_seats as number_of_seats_available,
            case
                when is_primary = 'false'
                then 'General'
                when is_primary = 'true'
                then 'Primary'
            end as election_type,
            case
                when parties like '%Independent%'
                then 'Independent'
                when parties like '%Nonpartisan%'
                then 'Nonpartisan'
                else ''
            end as party_affiliation,
            concat_ws(
                '|',
                transform(
                    from_json(
                        regexp_replace(parties, '=>', ':'),
                        'array<struct<name:string, short_name:string>>'
                    ),
                    x -> x.name
                )
            ) as party_list,
            parties,
            first_name as first_name,
            middle_name as middle_name,
            last_name as last_name,
            case
                when trim(upper(state)) = 'AK'
                then 'Alaska'
                when trim(upper(state)) = 'AL'
                then 'Alabama'
                when trim(upper(state)) = 'AR'
                then 'Arkansas'
                when trim(upper(state)) = 'AZ'
                then 'Arizona'
                when trim(upper(state)) = 'CA'
                then 'California'
                when trim(upper(state)) = 'CO'
                then 'Colorado'
                when trim(upper(state)) = 'CT'
                then 'Connecticut'
                when trim(upper(state)) = 'DC'
                then 'District of Columbia'
                when trim(upper(state)) = 'DE'
                then 'Delaware'
                when trim(upper(state)) = 'FL'
                then 'Florida'
                when trim(upper(state)) = 'GA'
                then 'Georgia'
                when trim(upper(state)) = 'HI'
                then 'Hawaii'
                when trim(upper(state)) = 'IA'
                then 'Iowa'
                when trim(upper(state)) = 'ID'
                then 'Idaho'
                when trim(upper(state)) = 'IL'
                then 'Illinois'
                when trim(upper(state)) = 'IN'
                then 'Indiana'
                when trim(upper(state)) = 'KS'
                then 'Kansas'
                when trim(upper(state)) = 'KY'
                then 'Kentucky'
                when trim(upper(state)) = 'LA'
                then 'Louisiana'
                when trim(upper(state)) = 'MA'
                then 'Massachusetts'
                when trim(upper(state)) = 'MD'
                then 'Maryland'
                when trim(upper(state)) = 'ME'
                then 'Maine'
                when trim(upper(state)) = 'MI'
                then 'Michigan'
                when trim(upper(state)) = 'MN'
                then 'Minnesota'
                when trim(upper(state)) = 'MO'
                then 'Missouri'
                when trim(upper(state)) = 'MS'
                then 'Mississippi'
                when trim(upper(state)) = 'MT'
                then 'Montana'
                when trim(upper(state)) = 'NC'
                then 'North Carolina'
                when trim(upper(state)) = 'ND'
                then 'North Dakota'
                when trim(upper(state)) = 'NE'
                then 'Nebraska'
                when trim(upper(state)) = 'NH'
                then 'New Hampshire'
                when trim(upper(state)) = 'NJ'
                then 'New Jersey'
                when trim(upper(state)) = 'NM'
                then 'New Mexico'
                when trim(upper(state)) = 'NV'
                then 'Nevada'
                when trim(upper(state)) = 'NY'
                then 'New York'
                when trim(upper(state)) = 'OH'
                then 'Ohio'
                when trim(upper(state)) = 'OK'
                then 'Oklahoma'
                when trim(upper(state)) = 'OR'
                then 'Oregon'
                when trim(upper(state)) = 'PA'
                then 'Pennsylvania'
                when trim(upper(state)) = 'RI'
                then 'Rhode Island'
                when trim(upper(state)) = 'SC'
                then 'South Carolina'
                when trim(upper(state)) = 'SD'
                then 'South Dakota'
                when trim(upper(state)) = 'TN'
                then 'Tennessee'
                when trim(upper(state)) = 'TX'
                then 'Texas'
                when trim(upper(state)) = 'UT'
                then 'Utah'
                when trim(upper(state)) = 'VA'
                then 'Virginia'
                when trim(upper(state)) = 'VT'
                then 'Vermont'
                when trim(upper(state)) = 'WA'
                then 'Washington'
                when trim(upper(state)) = 'WI'
                then 'Wisconsin'
                when trim(upper(state)) = 'WV'
                then 'West Virginia'
                when trim(upper(state)) = 'WY'
                then 'Wyoming'
                else state
            end as state,
            trim(regexp_replace(phone, '[^0-9]', '')) as phone,
            email,
            {{ extract_city_from_office_name("official_office_name") }} as city,
            case
                when official_office_name like '%- District %'
                then regexp_extract(official_office_name, '- District (.*)$')
                when official_office_name like '% - Ward %'
                then regexp_extract(official_office_name, ' - Ward (.*)$')
                when official_office_name like '% - Place %'
                then regexp_extract(official_office_name, ' - Place (.*)$')
                when official_office_name like '% - Branch %'
                then regexp_extract(official_office_name, ' - Branch (.*)$')
                when official_office_name like '% - Subdistrict %'
                then regexp_extract(official_office_name, ' - Subdistrict (.*)$')
                when official_office_name like '% - Zone %'
                then regexp_extract(official_office_name, ' - Zone (.*)$')
                else ''
            end as district,
            case
                when official_office_name like '% - Seat %'
                then regexp_extract(official_office_name, ' - Seat ([^,]+)')
                when official_office_name like '% - Group %'
                then regexp_extract(official_office_name, ' - Group ([^,]+)')
                when official_office_name like '%, Seat %'
                then regexp_extract(official_office_name, ', Seat ([^,]+)')
                when official_office_name like '%: % - Seat %'
                then regexp_extract(official_office_name, ' - Seat ([^,]+)')
                when official_office_name like '% - Position %'
                then regexp_extract(official_office_name, ' - Position ([^\\s(]+)')
                else ''
            end as seat,
            {{
                generate_candidate_slug(
                    "first_name", "last_name", "official_office_name"
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
                when election_day is null
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
                        regexp_replace(trim(election_day), ' ', '-'),
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
                                regexp_replace(trim(election_day), ' ', '-'),
                                '[^a-zA-Z0-9-]',
                                ''
                            )
                        )
                    )
            end as br_contest_id,
            candidacy_id,
            race_id as ballotready_race_id,
            cast(position_id as int) as position_id,
            parties,
            candidacy_created_at,
            candidacy_updated_at
        from br_new
        -- what follows is the core substance of who is being selected for uploading
        -- to HubSpot
        where
            -- separate the records with both phones and emails
            phone <> ''
            and email <> ''
            -- remove any major party candidates
            and not parties like '%Democrat%'
            and not parties like '%Republican%'
            -- subset to candidates with future elections
            and election_day > current_date
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
