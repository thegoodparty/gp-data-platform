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
--
-- BR enrichment via br_race_id mirrors int__civics_candidacy_techspeed and
-- int__civics_election_techspeed: same date-coalesce in source so the stage
-- date drives a consistent gp_election_stage_id, and brp.br_gp_election_id
-- as a fallback so TS-only stages adopt BR's gp_election_id.
with
    br_race_to_position as ({{ br_race_to_position_lookup() }}),

    br_election_dates as (
        select
            gp_election_id as br_gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as br_primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as br_general_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id
    ),

    -- ER crosswalk — joined twice below:
    -- (1) stage grain: canonical_gp_election_stage_id only applies to the
    -- specific clustered stage.
    -- (2) candidacy grain: canonical_gp_election_id applies to ANY stage of a
    -- matched candidacy, matching int__civics_election_techspeed's
    -- candidacy-grain join (required for FK relationships to hold).
    canonical_stage as (
        select
            ts_source_candidate_id,
            ts_stage_election_date,
            canonical_gp_election_stage_id
        from {{ ref("int__civics_er_canonical_ids") }}
    ),

    canonical_candidacy as (
        select ts_source_candidate_id, canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
        qualify
            row_number() over (
                partition by ts_source_candidate_id order by canonical_gp_election_id
            )
            = 1
    ),

    source as (
        select
            ts.* except (
                state,
                is_primary,
                primary_election_date_parsed,
                general_election_date_parsed
            ),
            -- Aliases for generate_gp_election_id macro compatibility
            ts.state_postal_code as state,
            cast(null as string) as seat_name,
            -- Mirror the conditional BR-fallback substitution used in
            -- int__civics_candidacy_techspeed and int__civics_election_techspeed:
            -- only fill in dates when TS has neither, so the stage_type
            -- determination for rows with at least one TS date stays unchanged.
            case
                when
                    ts.primary_election_date_parsed is not null
                    or ts.general_election_date_parsed is not null
                then ts.primary_election_date_parsed
                else bed.br_primary_election_date
            end as primary_election_date_parsed,
            case
                when
                    ts.primary_election_date_parsed is not null
                    or ts.general_election_date_parsed is not null
                then ts.general_election_date_parsed
                else bed.br_general_election_date
            end as general_election_date_parsed,
            brp.br_gp_election_id,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join br_race_to_position as brp on ts.br_race_id = brp.br_race_id
        left join
            br_election_dates as bed on brp.br_gp_election_id = bed.br_gp_election_id
    ),

    -- Determine stage type: primary takes priority over general. If TechSpeed
    -- populates both dates (or BR enrichment fills in the missing one), the
    -- candidate is at the primary stage.
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

    -- canonical_gp_election_stage_id: stage-grain join (only populated if THIS
    -- specific race's stage was clustered).
    -- canonical_gp_election_id: candidacy-grain join, then propagated across
    -- ALL raw rows of the same election via window. Matches the cascade in
    -- int__civics_election_techspeed (both use candidacy-grain xw), ensuring
    -- gp_election_id alignment between the two models.
    with_stage_and_canonical as (
        select
            with_stage.*,
            xw_stage.canonical_gp_election_stage_id,
            max(xw_cand.canonical_gp_election_id) over (
                partition by {{ generate_gp_election_id() }}
            ) as canonical_gp_election_id
        from with_stage
        left join
            canonical_stage as xw_stage
            on with_stage.techspeed_candidate_code = xw_stage.ts_source_candidate_id
            and with_stage.stage_election_date = xw_stage.ts_stage_election_date
        left join
            canonical_candidacy as xw_cand
            on with_stage.techspeed_candidate_code = xw_cand.ts_source_candidate_id
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
            -- BR-derived gp_election_id (from br_race_id lookup) — used as
            -- fallback when ER didn't cluster. Per-race so safe to max over.
            max(br_gp_election_id) as br_gp_election_id,
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
                canonical_gp_election_stage_id, {{ generate_ts_gp_election_stage_id() }}
            ) as gp_election_stage_id,

            coalesce(
                canonical_gp_election_id,
                br_gp_election_id,
                {{ generate_gp_election_id() }}
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
