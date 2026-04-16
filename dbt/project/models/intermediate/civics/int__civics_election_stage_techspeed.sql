{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart election_stage schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates
--
-- Grain: One row per election stage (race + stage type)
--
-- A TechSpeed candidate has both primary_election_date AND general_election_date,
-- but only ONE br_race_id. We unpivot primary/general dates into separate stage
-- rows, then aggregate to race-level.
--
-- gp_election_stage_id is generated from a composite key (NOT br_race_id) because
-- a single br_race_id would collide across the two stage types.
with
    -- ER crosswalk: for clustered TS stages, adopt BR's canonical election_stage
    -- and election IDs. Keyed at stage grain.
    canonical as (
        select
            ts_source_candidate_id,
            ts_stage_election_date,
            canonical_gp_election_stage_id,
            canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
    ),

    source as (
        select
            ts.* except (state, is_primary),
            -- Aliases for generate_gp_election_id macro compatibility
            state_postal_code as state,
            cast(null as string) as seat_name,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state_postal_code",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    -- Determine stage type: primary takes priority over general. If TechSpeed
    -- populates both dates, the candidate is at the primary stage.
    with_stage as (
        select
            *,
            case
                when primary_election_date_parsed is not null
                then 'primary'
                else 'general'
            end as stage_type,
            case
                when primary_election_date_parsed is not null
                then primary_election_date_parsed
                else general_election_date_parsed
            end as stage_election_date,
            primary_election_date_parsed is not null as is_primary
        from source
        where
            coalesce(primary_election_date_parsed, general_election_date_parsed)
            is not null
            and year(
                coalesce(primary_election_date_parsed, general_election_date_parsed)
            )
            between 1900 and 2050
    ),

    -- Join crosswalk at stage grain so any candidate in a matched race carries
    -- the canonical IDs downstream to the race-level aggregation.
    with_stage_and_canonical as (
        select
            with_stage.*, xw.canonical_gp_election_stage_id, xw.canonical_gp_election_id
        from with_stage
        left join
            canonical as xw
            on with_stage.techspeed_candidate_code = xw.ts_source_candidate_id
            and with_stage.stage_election_date = xw.ts_stage_election_date
    ),

    -- Aggregate to race-level (one row per race + stage, not per candidate)
    race_aggregated as (
        select
            state,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            district,
            city,
            seat_name,
            stage_election_date,
            stage_type,
            is_primary,
            -- These fields are race-level (same across all candidates in a race).
            -- any_value() is safe here because the GROUP BY already partitions by
            -- all race-defining fields; these are just race attributes carried along.
            any_value(election_date) as election_date,
            any_value(seats_available) as seats_available,
            any_value(br_race_id) as br_race_id,
            any_value(is_partisan) as is_partisan,
            -- Propagate xw canonicals to the whole race: if ANY candidate in
            -- this race matched to BR, the race adopts BR's IDs.
            max(canonical_gp_election_stage_id) as canonical_gp_election_stage_id,
            max(canonical_gp_election_id) as canonical_gp_election_id,
            min(_airbyte_extracted_at) as created_at,
            max(_airbyte_extracted_at) as updated_at
        from with_stage_and_canonical
        group by
            state,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            district,
            city,
            seat_name,
            stage_election_date,
            stage_type,
            is_primary
    ),

    election_stages as (
        select
            coalesce(
                canonical_gp_election_stage_id,
                {{
                    generate_salted_uuid(
                        fields=[
                            "'techspeed'",
                            "state",
                            "candidate_office",
                            "official_office_name",
                            "district",
                            "city",
                            "cast(stage_election_date as string)",
                            "stage_type",
                        ]
                    )
                }}
            ) as gp_election_stage_id,

            coalesce(
                canonical_gp_election_id, {{ generate_gp_election_id() }}
            ) as gp_election_id,

            cast(br_race_id as string) as br_race_id,
            cast(null as string) as br_election_id,
            cast(null as bigint) as br_position_id,
            cast(null as string) as ddhq_race_id,

            stage_type,
            stage_election_date as election_date,

            state
            || ' '
            || cast(year(stage_election_date) as string)
            || ' '
            || initcap(stage_type) as election_name,
            state || ' ' || official_office_name as race_name,

            is_primary,
            false as is_runoff,
            false as is_retention,
            seats_available as number_of_seats,
            cast(null as string) as total_votes_cast,
            is_partisan,
            cast(null as date) as filing_period_start_on,
            cast(null as date) as filing_period_end_on,
            cast(null as string) as filing_requirements,
            cast(null as string) as filing_address,
            cast(null as string) as filing_phone,
            created_at,
            updated_at
        from race_aggregated
        where election_date is not null
    ),

    deduplicated as (
        select *
        from election_stages
        qualify
            row_number() over (
                partition by gp_election_stage_id order by updated_at desc
            )
            = 1
    )

select *
from deduplicated
