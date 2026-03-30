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
    clean_states as (select * from {{ ref("clean_states") }}),

    source as (
        select
            ts.* except (state, is_primary),
            coalesce(cs.state_cleaned_postal_code, ts.state) as state,
            -- Add missing columns required by generate_gp_election_id macro
            cast(null as string) as seat_name,
            try_cast(number_of_seats_available as int) as seats_available,
            -- Parse primary_election_date
            coalesce(
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as primary_election_date_parsed,
            -- Parse general_election_date
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy')
            ) as general_election_date_parsed,
            -- election_date for generate_gp_election_id macro (coalesce general,
            -- primary)
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as election_date
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
    ),

    -- Unpivot: one row per candidate per stage date
    primary_stages as (
        select
            *,
            'primary' as stage_type,
            primary_election_date_parsed as stage_election_date,
            true as is_primary
        from source
        where
            primary_election_date_parsed is not null
            and year(primary_election_date_parsed) between 1900 and 2030
    ),

    general_stages as (
        select
            *,
            'general' as stage_type,
            general_election_date_parsed as stage_election_date,
            false as is_primary
        from source
        where
            general_election_date_parsed is not null
            and year(general_election_date_parsed) between 1900 and 2030
    ),

    unpivoted as (
        select *
        from primary_stages
        union all
        select *
        from general_stages
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
            any_value(partisan) as partisan_type,
            min(_airbyte_extracted_at) as created_at,
            max(_airbyte_extracted_at) as updated_at
        from unpivoted
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
            -- gp_election_stage_id: composite key (NOT br_race_id)
            {{
                generate_salted_uuid(
                    fields=[
                        "'techspeed'",
                        "state",
                        "candidate_office",
                        "district",
                        "city",
                        "cast(stage_election_date as string)",
                        "stage_type",
                    ]
                )
            }} as gp_election_stage_id,

            -- gp_election_id: same logic as candidacy model
            {{ generate_gp_election_id() }} as gp_election_id,

            -- Source IDs
            cast(br_race_id as string) as br_race_id,
            cast(null as string) as br_election_id,
            cast(null as bigint) as br_position_id,
            cast(null as string) as ddhq_race_id,

            stage_type,
            stage_election_date as election_date,

            -- Constructed names
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
            partisan_type,
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
