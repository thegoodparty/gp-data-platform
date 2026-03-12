-- BallotReady election stages → Civics mart election_stage schema
-- Source: BallotReady API (race + election + position tables)
--
-- Grain: One row per election stage (position + election + stage type)
--
-- A BallotReady "race" maps to an "election stage" — each race represents a
-- single stage (primary, general, or runoff) for a position within an election.
--
-- This model is the foundation for int__civics_election_ballotready, which
-- rolls up election stages into elections.
with
    race as (select * from {{ ref("stg_airbyte_source__ballotready_api_race") }}),

    br_election as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_api_election") }}
        where election_day >= '2026-01-01'
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    br_normalized as (select * from {{ ref("int__ballotready_normalized_position") }}),

    br_filing_period as (select * from {{ ref("int__ballotready_filing_period") }}),

    -- Pick one filing period per race. As of 2026-03-04, only 6 of 547K races
    -- have >1 filing period; take the highest database_id (most recent).
    filing_period_ids as (
        select
            race.database_id as race_database_id,
            max(fp.databaseid) as filing_period_database_id
        from race
        lateral view explode(filing_periods) as fp
        group by race_database_id
    ),

    races_with_fields as (
        select
            race.*,
            br_election.election_day,
            br_election.name as election_name,
            br_election.database_id as br_election_database_id,
            br_position.name as position_name,
            br_position.state,
            br_position.level,
            br_position.database_id as br_position_database_id,
            br_position.sub_area_name,
            br_position.sub_area_value,
            br_position.retention as is_retention,
            br_position.seats as position_seats,
            br_position.name as official_office_name,
            {{
                generate_candidate_office_from_position(
                    "br_position.name",
                    "br_normalized.name",
                )
            }} as candidate_office,
            initcap(br_position.level) as office_level,
            {{
                map_ballotready_office_type(
                    generate_candidate_office_from_position(
                        "br_position.name",
                        "br_normalized.name",
                    )
                )
            }} as office_type,
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
            br_position.partisan_type,
            br_position.filing_requirements,
            br_position.filing_address,
            br_position.filing_phone,
            br_filing_period.start_on as filing_period_start_on,
            br_filing_period.end_on as filing_period_end_on
        from race
        inner join br_election on race.election.databaseid = br_election.database_id
        inner join br_position on race.position.databaseid = br_position.database_id
        left join
            br_normalized
            on br_position.normalized_position.databaseid = br_normalized.database_id
        left join
            filing_period_ids as fp_ids on race.database_id = fp_ids.race_database_id
        left join
            br_filing_period
            on fp_ids.filing_period_database_id = br_filing_period.database_id
    ),

    -- To generate gp_election_id we need the GENERAL election date for this
    -- position, not the stage-specific date. Look it up from the general-stage
    -- rows by position + year.
    -- NOTE: primaries and generals have different br_election_database_id values
    -- (e.g. "2026 WA Primary" vs "2026 WA General"), so we match by election
    -- year instead to ensure primary stages find their corresponding general date.
    general_election_dates as (
        select
            br_position_database_id,
            year(election_day) as election_year,
            max(election_day) as general_election_date
        from races_with_fields
        where not is_primary and not is_runoff
        group by br_position_database_id, year(election_day)
    ),

    election_stages as (
        select
            -- gp_election_stage_id from br race database_id
            {{ generate_salted_uuid(fields=["races_with_fields.database_id"]) }}
            as gp_election_stage_id,

            -- gp_election_id needs the general election date, not the stage date
            {{ generate_gp_election_id("elec_date_lookup") }} as gp_election_id,

            -- BallotReady source identifiers
            cast(races_with_fields.database_id as string) as br_race_id,
            cast(races_with_fields.br_election_database_id as string) as br_election_id,
            races_with_fields.br_position_database_id as br_position_id,
            cast(null as string) as ddhq_race_id,

            -- Map stage type
            case
                when races_with_fields.is_runoff and races_with_fields.is_primary
                then 'primary runoff'
                when races_with_fields.is_runoff and not races_with_fields.is_primary
                then 'general runoff'
                when races_with_fields.is_primary
                then 'primary'
                else 'general'
            end as stage_type,

            races_with_fields.election_day as election_date,

            -- election_name: the overarching election event name from BallotReady
            races_with_fields.election_name as election_name,

            -- race_name: office-specific label constructed from position fields
            races_with_fields.state
            || ' '
            || races_with_fields.position_name
            || case
                when
                    nullif(races_with_fields.sub_area_name, '') is not null
                    and nullif(races_with_fields.sub_area_value, '') is not null
                then
                    ', '
                    || races_with_fields.sub_area_name
                    || ' '
                    || races_with_fields.sub_area_value
                else ''
            end as race_name,

            races_with_fields.is_primary,
            races_with_fields.is_runoff,
            races_with_fields.is_retention,
            races_with_fields.seats as number_of_seats,
            cast(null as string) as total_votes_cast,
            races_with_fields.partisan_type,
            races_with_fields.filing_period_start_on,
            races_with_fields.filing_period_end_on,
            races_with_fields.filing_requirements,
            races_with_fields.filing_address,
            races_with_fields.filing_phone,
            races_with_fields.updated_at as created_at,
            races_with_fields.updated_at as updated_at

        from races_with_fields
        -- Join to get the general election date for this position + year
        left join
            general_election_dates as ged
            on races_with_fields.br_position_database_id = ged.br_position_database_id
            and year(races_with_fields.election_day) = ged.election_year
        -- Build a virtual row with the general election date for the macro
        cross join
            lateral(
                select
                    races_with_fields.official_office_name,
                    races_with_fields.candidate_office,
                    races_with_fields.office_level,
                    races_with_fields.office_type,
                    races_with_fields.state,
                    races_with_fields.city,
                    races_with_fields.district,
                    races_with_fields.seat_name,
                    coalesce(
                        ged.general_election_date, races_with_fields.election_day
                    ) as election_date,
                    races_with_fields.seats as seats_available
            ) as elec_date_lookup
    ),

    deduplicated as (
        select *
        from election_stages
        qualify
            row_number() over (
                partition by gp_election_stage_id order by created_at desc
            )
            = 1
    )

select
    gp_election_stage_id,
    gp_election_id,
    br_race_id,
    br_election_id,
    br_position_id,
    ddhq_race_id,
    stage_type,
    election_date,
    election_name,
    race_name,
    is_primary,
    is_runoff,
    is_retention,
    number_of_seats,
    total_votes_cast,
    partisan_type,
    filing_period_start_on,
    filing_period_end_on,
    filing_requirements,
    filing_address,
    filing_phone,
    created_at,
    updated_at
from deduplicated
