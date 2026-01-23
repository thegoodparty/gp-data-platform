{{
    config(
        materialized="table",
        tags=["mart", "civics", "election_stage"],
    )
}}

-- Election Stages: One row per election stage (race)
-- Example: "Seattle Mayor 2026 Primary", "Seattle Mayor 2026 General" = separate rows
-- Values: Primary, General, Runoff
with
    source as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    election_stages as (
        select
            {{
                generate_salted_uuid(
                    fields=["ballotready_race_id"], salt="civics_election_stage"
                )
            }} as gp_election_stage_id,

            -- FK to parent election
            {{
                generate_salted_uuid(
                    fields=[
                        "ballotready_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_election",
                )
            }} as gp_election_id,

            -- BallotReady identifiers
            ballotready_race_id as br_race_id,

            -- DDHQ placeholder
            cast(null as bigint) as ddhq_race_id,

            -- Stage info
            election_type as stage_type,
            election_date,
            election_stage_name,

            -- Stage type flags
            is_primary,
            is_runoff,
            is_retention,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where ballotready_race_id is not null
        qualify
            row_number() over (
                partition by ballotready_race_id order by candidacy_updated_at desc
            )
            = 1
    )

select *
from election_stages
