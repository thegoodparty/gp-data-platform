-- BallotReady elections → Civics mart election schema
-- Source: int__civics_election_stage_ballotready (rolled up from API race +
-- election + position tables)
--
-- Grain: One row per election (position + election date)
--
-- An "election" represents the full cycle for a specific position in a specific
-- year, encompassing all stages (primary, general, runoff). We roll up from
-- election stages, using gp_election_id as the grouping key. This ensures that
-- positions with only a primary or runoff (no general race yet) are still
-- included.
with
    stages as (select * from {{ ref("int__civics_election_stage_ballotready") }}),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    br_normalized as (select * from {{ ref("int__ballotready_normalized_position") }}),

    -- Pick one representative stage per election to derive election-level fields.
    -- Prefer the general stage; fall back to the earliest stage otherwise.
    representative_stage as (
        select *
        from stages
        qualify
            row_number() over (
                partition by gp_election_id
                order by
                    case stage_type when 'general' then 0 else 1 end, election_date asc
            )
            = 1
    ),

    election_base as (
        select
            rs.gp_election_id,

            -- Derive fields from position data
            br_position.name as official_office_name,
            {{
                generate_candidate_office_from_position(
                    "br_position.name",
                    "br_normalized.name",
                )
            }} as candidate_office,
            initcap(br_position.level) as office_level,
            {{
                map_office_type(
                    generate_candidate_office_from_position(
                        "br_position.name",
                        "br_normalized.name",
                    )
                )
            }} as office_type,
            br_position.state,
            {{ extract_city_from_office_name("br_position.name") }} as city,
            coalesce(
                regexp_extract(
                    br_position.name,
                    '- (?:District|Ward|Place|Branch|Subdistrict|Zone) (.+)$'
                ),
                ''
            ) as district,
            coalesce(
                regexp_extract(br_position.name, '[-, ] (?:Seat|Group) ([^,]+)'),
                regexp_extract(br_position.name, ' - Position ([^\\s(]+)'),
                ''
            ) as seat_name,

            -- Use the general election date if available, otherwise the
            -- representative stage date (already embedded in gp_election_id)
            rs.election_date,
            year(rs.election_date) as election_year,

            -- Election metadata
            cast(null as date) as filing_deadline,
            cast(null as int) as population,
            rs.number_of_seats as seats_available,
            cast(null as date) as term_start_date,
            cast(null as boolean) as is_uncontested,
            cast(null as string) as number_of_opponents,
            cast(null as boolean) as is_open_seat,
            false as has_ddhq_match,

            -- BallotReady enrichment
            rs.br_position_id as br_position_database_id,
            br_position.is_judicial,
            br_position.is_appointed,
            br_normalized.name as br_normalized_position_type,

            -- Timestamps
            rs.created_at,
            rs.updated_at

        from representative_stage as rs
        inner join br_position on rs.br_position_id = br_position.database_id
        left join
            br_normalized
            on br_position.normalized_position.databaseid = br_normalized.database_id
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
    is_open_seat,
    has_ddhq_match,
    br_position_database_id,
    is_judicial,
    is_appointed,
    br_normalized_position_type,
    created_at,
    updated_at
from election_base
