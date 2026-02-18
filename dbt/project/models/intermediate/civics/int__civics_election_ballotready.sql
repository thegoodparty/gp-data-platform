-- BallotReady elections → Civics mart election schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
-- Enriched with BallotReady API position and normalized position data
--
-- Grain: One row per election (position + election date)
--
-- An "election" represents the full cycle for a specific position in a specific
-- year, encompassing all stages (primary, general, runoff). We derive the
-- election from the general-stage candidacy records (is_primary = false,
-- is_runoff = false) since those represent the culminating election date.
with
    source as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where
            election_day >= '2026-01-01'
            -- Use general election records to define elections
            -- (primaries/runoffs are stages within an election)
            and is_primary = 'false'
            and is_runoff = 'false'
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    br_normalized as (select * from {{ ref("int__ballotready_normalized_position") }}),

    election_base as (
        select
            -- Derive fields needed for generate_gp_election_id macro
            source.position_name as official_office_name,
            {{
                generate_candidate_office_from_position(
                    "source.position_name", "source.normalized_position_name"
                )
            }} as candidate_office,
            initcap(source.level) as office_level,
            {{
                map_ballotready_office_type(
                    generate_candidate_office_from_position(
                        "source.position_name", "source.normalized_position_name"
                    )
                )
            }} as office_type,
            source.state,
            {{ extract_city_from_office_name("source.position_name") }} as city,
            coalesce(
                regexp_extract(
                    source.position_name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district,
            coalesce(
                regexp_extract(source.position_name, '[-, ] (?:Seat|Group) ([^,]+)'),
                regexp_extract(source.position_name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,
            cast(source.election_day as date) as election_date,
            year(cast(source.election_day as date)) as election_year,

            -- Election metadata (from position API)
            cast(null as date) as filing_deadline,
            cast(null as int) as population,
            cast(source.number_of_seats as int) as seats_available,
            cast(null as date) as term_start_date,
            cast(null as string) as is_uncontested,
            cast(null as string) as number_of_opponents,
            cast(null as string) as open_seat,
            -- We should probably refactor this schema slightly and might not
            -- need an indicator for `ddhq_match`, but we're hardcoding this as
            -- false for now until we update our record linkage against ddhq
            -- data
            false as has_ddhq_match,

            -- BallotReady enrichment
            cast(source.br_position_id as int) as br_position_database_id,
            br_position.judicial as is_judicial,
            br_position.appointed as is_appointed,
            br_normalized.name as br_normalized_position_type,

            -- Timestamps
            source._airbyte_extracted_at as created_at,
            source._airbyte_extracted_at as updated_at

        from source
        left join
            br_position on cast(source.br_position_id as int) = br_position.database_id
        left join
            br_normalized
            on br_position.normalized_position.databaseid = br_normalized.database_id
    ),

    elections_with_id as (
        select {{ generate_gp_election_id() }} as gp_election_id, * from election_base
    ),

    deduplicated as (
        select *
        from elections_with_id
        qualify
            row_number() over (partition by gp_election_id order by updated_at desc) = 1
    )

select
    gp_election_id,
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    seat_name,
    election_date,
    election_year,
    filing_deadline,
    population,
    seats_available,
    term_start_date,
    is_uncontested,
    number_of_opponents,
    open_seat,
    has_ddhq_match,
    br_position_database_id,
    is_judicial,
    is_appointed,
    br_normalized_position_type,
    created_at,
    updated_at
from deduplicated
