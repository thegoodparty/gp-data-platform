-- BallotReady candidacies → Civics mart candidacy schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
--
-- Grain: One row per candidacy (candidate + position + election year)
--
-- The BallotReady S3 data is at the RACE grain (one row per candidate per stage:
-- primary, general, runoff). We roll up to the CANDIDACY grain by grouping on
-- candidate + position + election, then extracting stage-specific dates.
--
-- UUID fields MUST match int__civics_candidacy_2025 / int__civics_candidacy_techspeed
-- to ensure same candidacy from different sources gets same gp_candidacy_id
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    -- Get enriched person data from BallotReady API (has contact info)
    br_person as (select * from {{ ref("int__ballotready_person") }}),

    person_emails as (
        select
            database_id as person_database_id,
            get(filter(contacts, x -> x.email is not null), 0).email as api_email
        from br_person
        where database_id is not null
    ),

    -- Derive fields needed for ID generation at the race (row) level
    candidacies_with_fields as (
        select
            candidacies.*,
            person_emails.api_email,
            {{
                generate_candidate_office_from_position(
                    "candidacies.position_name",
                    "candidacies.normalized_position_name",
                )
            }} as candidate_office,
            initcap(candidacies.level) as office_level,
            {{ extract_city_from_office_name("candidacies.position_name") }} as city,
            coalesce(
                regexp_extract(
                    candidacies.position_name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district,
            coalesce(
                regexp_extract(
                    candidacies.position_name, '[-, ] (?:Seat|Group) ([^,]+)'
                ),
                regexp_extract(candidacies.position_name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,
            br_position.partisan_type
        from candidacies
        left join br_position on candidacies.br_position_id = br_position.database_id
        left join
            person_emails
            on candidacies.br_candidate_id = person_emails.person_database_id
    ),

    -- Roll up from race-level to candidacy-level
    -- Group by candidate + position + election YEAR (not br_election_id, which
    -- differs between primary and general stages in BallotReady data)
    candidacy_rolled_up as (
        select
            -- Natural key for grouping stages into one candidacy
            br_candidate_id,
            br_position_id,
            year(election_day) as election_year,

            -- Take candidate fields from any row (they're the same across stages)
            any_value(first_name) as first_name,
            any_value(last_name) as last_name,
            any_value(state) as state,
            any_value(email) as email,
            any_value(api_email) as api_email,
            any_value(phone) as phone,
            any_value(position_name) as official_office_name,
            any_value(candidate_office) as candidate_office,
            any_value(office_level) as office_level,
            any_value(normalized_position_name) as normalized_position_name,
            any_value(city) as city,
            any_value(district) as district,
            any_value(seat_name) as seat_name,
            any_value(parties) as parties,
            any_value(partisan_type) as partisan_type,
            any_value(number_of_seats) as seats_available,
            any_value(_airbyte_extracted_at) as _airbyte_extracted_at,

            -- Extract stage-specific dates
            max(case when is_primary then election_day end) as primary_election_date,
            max(
                case when not is_primary and not is_runoff then election_day end
            ) as general_election_date,
            max(case when is_runoff then election_day end) as runoff_election_date,

            -- The general election result is the canonical candidacy result
            -- Fall back to primary result if no general yet
            coalesce(
                max(
                    case when not is_primary and not is_runoff then election_result end
                ),
                max(case when is_primary then election_result end)
            ) as raw_election_result,

            max(candidacy_updated_at) as candidacy_updated_at

        from candidacies_with_fields
        group by br_candidate_id, br_position_id, year(election_day)
    ),

    -- Look up the general election date and seats from the election_stage model
    -- (which uses the API race data and has the same year-based general-date
    -- lookup). This ensures the candidacy model computes the same
    -- gp_election_id as the election and election_stage tables.
    general_election_date_lookup as (
        select
            br_position_id,
            year(election_date) as election_year,
            max(election_date) as general_election_date,
            any_value(number_of_seats) as seats_available
        from {{ ref("int__civics_election_stage_ballotready") }}
        where not is_primary and not is_runoff
        group by br_position_id, year(election_date)
    ),

    candidacies_enriched as (
        select
            -- For gp_election_id, we need the general election date
            -- First try the election_stage model's general date (source of
            -- truth), then the candidacy's own general date from S3, then
            -- fall back to primary/runoff date
            coalesce(
                ged.general_election_date,
                rolled.general_election_date,
                rolled.primary_election_date,
                rolled.runoff_election_date
            ) as election_date,

            -- seats_available from the election_stage model (API race data) for
            -- consistency with gp_election_id generation in the election models
            coalesce(ged.seats_available, rolled.seats_available) as seats_available,

            -- Parse party from parties JSON
            -- parties format: [{"name"=>"Nonpartisan", "short_name"=>"NP"}]
            {{ parse_party_affiliation("parties") }} as party_affiliation,

            -- Map election_result to candidacy_result
            case
                when raw_election_result in ('WON', 'GENERAL_WIN')
                then 'Won'
                when raw_election_result in ('LOST', 'LOSS')
                then 'Lost'
                when raw_election_result = 'PRIMARY_WIN'
                then 'Won'
                when raw_election_result = 'RUNOFF'
                then 'Runoff'
                when raw_election_result = ''
                then null
                else null
            end as candidacy_result,

            case
                when partisan_type = 'partisan'
                then 'Partisan'
                when partisan_type = 'nonpartisan'
                then 'Nonpartisan'
                else null
            end as is_partisan,

            -- Compute office_type here so it's available for generate_gp_election_id
            {{ map_ballotready_office_type("candidate_office") }} as office_type,

            rolled.* except (seats_available)

        from candidacy_rolled_up as rolled
        left join
            general_election_date_lookup as ged
            on rolled.br_position_id = ged.br_position_id
            and rolled.election_year = ged.election_year
    ),

    candidacies_with_ids as (
        select
            -- gp_candidacy_id - matches HubSpot/TechSpeed pattern
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "party_affiliation",
                        "candidate_office",
                        "cast(coalesce(general_election_date, primary_election_date, runoff_election_date) as string)",
                        "district",
                    ]
                )
            }}
            as gp_candidacy_id,

            -- gp_candidate_id - matches int__civics_candidate_ballotready pattern
            -- Must use coalesce(email, api_email) to match the candidate model
            {{
                generate_salted_uuid(
                    fields=[
                        "first_name",
                        "last_name",
                        "state",
                        "cast(null as string)",
                        "coalesce(email, api_email)",
                        "phone",
                    ]
                )
            }} as gp_candidate_id,

            -- gp_election_id - use the generate_gp_election_id macro
            -- The macro expects columns without table prefix in the current scope
            {{ generate_gp_election_id() }} as gp_election_id,

            -- External IDs (NULL for BR-sourced records until we link them in
            -- followup work)
            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,

            -- Source tracking
            'ballotready' as candidate_id_source,

            -- Candidacy attributes
            party_affiliation,
            -- BallotReady does not provide incumbent or open seat data
            cast(null as string) as is_incumbent,
            cast(null as string) as is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            -- Hardcoded until we link BallotReady candidacies with Product DB
            cast(null as boolean) as is_pledged,
            cast(null as boolean) as is_verified,
            cast(null as string) as verification_status_reason,
            is_partisan,
            primary_election_date,
            general_election_date,
            runoff_election_date,

            -- BallotReady position ID
            br_position_id as br_position_database_id,

            -- Assessment fields (hardcoded until we join viability/p2v in followup
            -- work)
            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            -- Timestamps
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from candidacies_enriched
        where
            -- Must have at least a general or primary election date for ID generation
            coalesce(general_election_date, primary_election_date, runoff_election_date)
            is not null
    ),

    -- Ensure referential integrity with candidate table
    valid_candidates as (
        select gp_candidate_id from {{ ref("int__civics_candidate_ballotready") }}
    ),

    filtered as (
        select candidacies_with_ids.*
        from candidacies_with_ids
        inner join
            valid_candidates
            on candidacies_with_ids.gp_candidate_id = valid_candidates.gp_candidate_id
    ),

    deduplicated as (
        select *
        from filtered
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    gp_candidacy_id,
    gp_candidate_id,
    gp_election_id,
    product_campaign_id,
    hubspot_contact_id,
    hubspot_company_ids,
    candidate_id_source,
    party_affiliation,
    is_incumbent,
    is_open_seat,
    candidate_office,
    official_office_name,
    office_level,
    office_type,
    candidacy_result,
    is_pledged,
    is_verified,
    verification_status_reason,
    is_partisan,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    br_position_database_id,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at
from deduplicated
