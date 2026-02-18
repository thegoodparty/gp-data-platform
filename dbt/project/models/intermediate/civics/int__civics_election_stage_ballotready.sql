-- BallotReady election stages → Civics mart election_stage schema
-- Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections)
--
-- Grain: One row per election stage (position + election + stage type)
--
-- A BallotReady "race" maps to an "election stage" — each race represents a
-- single stage (primary, general, or runoff) for a position within an election.
with
    candidacies as (
        select *
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where election_day >= '2026-01-01'
    ),

    br_position as (
        select * from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    candidacies_with_fields as (
        select
            candidacies.*,
            candidacies.position_name as official_office_name,
            {{
                generate_candidate_office_from_position(
                    "candidacies.position_name",
                    "candidacies.normalized_position_name",
                )
            }} as candidate_office,
            initcap(candidacies.level) as office_level,
            {{
                map_ballotready_office_type(
                    generate_candidate_office_from_position(
                        "candidacies.position_name",
                        "candidacies.normalized_position_name",
                    )
                )
            }} as office_type,
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
            ) as seat_name
        from candidacies
        left join
            br_position
            on cast(candidacies.br_position_id as int) = br_position.database_id
    ),

    -- To generate gp_election_id we need the GENERAL election date for this
    -- position+election, not the stage-specific date. Look it up from the
    -- general-stage rows.
    general_election_dates as (
        select
            br_position_id,
            br_election_id,
            max(cast(election_day as date)) as general_election_date
        from candidacies
        where is_primary = 'false' and is_runoff = 'false'
        group by br_position_id, br_election_id
    ),

    election_stages as (
        select
            -- gp_election_stage_id from br_race_id (analogous to ddhq_race_id pattern)
            {{ generate_salted_uuid(fields=["candidacies_with_fields.br_race_id"]) }}
            as gp_election_stage_id,

            -- gp_election_id needs the general election date, not the stage date
            -- We temporarily override election_date for the macro
            {{ generate_gp_election_id("elec_date_lookup") }} as gp_election_id,

            cast(null as string) as hubspot_contact_id,
            cast(candidacies_with_fields.br_race_id as string) as ddhq_race_id,

            -- Map stage type
            case
                when candidacies_with_fields.is_primary = 'true'
                then 'primary'
                when candidacies_with_fields.is_runoff = 'true'
                then 'runoff'
                else 'general'
            end as election_stage,

            cast(
                candidacies_with_fields.election_day as date
            ) as ddhq_election_stage_date,
            candidacies_with_fields.election_name as ddhq_race_name,
            cast(null as string) as total_votes_cast,
            candidacies_with_fields._airbyte_extracted_at as created_at

        from candidacies_with_fields
        -- Join to get the general election date for this position+election
        left join
            general_election_dates as ged
            on candidacies_with_fields.br_position_id = ged.br_position_id
            and candidacies_with_fields.br_election_id = ged.br_election_id
        -- Build a virtual row with the general election date for the macro
        cross join
            lateral(
                select
                    candidacies_with_fields.official_office_name,
                    candidacies_with_fields.candidate_office,
                    candidacies_with_fields.office_level,
                    candidacies_with_fields.office_type,
                    candidacies_with_fields.state,
                    candidacies_with_fields.city,
                    candidacies_with_fields.district,
                    candidacies_with_fields.seat_name,
                    coalesce(
                        ged.general_election_date,
                        cast(candidacies_with_fields.election_day as date)
                    ) as election_date
            ) as elec_date_lookup
    ),

    -- Only include stages that have a matching election
    valid_elections as (
        select gp_election_id from {{ ref("int__civics_election_ballotready") }}
    ),

    filtered as (
        select stage.*
        from election_stages as stage
        inner join
            valid_elections on stage.gp_election_id = valid_elections.gp_election_id
    ),

    deduplicated as (
        select *
        from filtered
        qualify
            row_number() over (
                partition by gp_election_stage_id order by created_at desc
            )
            = 1
    )

select
    gp_election_stage_id,
    gp_election_id,
    hubspot_contact_id,
    ddhq_race_id,
    election_stage,
    ddhq_election_stage_date,
    ddhq_race_name,
    total_votes_cast,
    created_at
from deduplicated
