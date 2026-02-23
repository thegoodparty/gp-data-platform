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
                    fields=["br_race_id"], salt="civics_election_stage"
                )
            }} as gp_election_stage_id,

            -- FK to parent election
            {{
                generate_salted_uuid(
                    fields=[
                        "br_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_election",
                )
            }} as gp_election_id,

            -- BallotReady identifiers
            br_race_id,
            br_election_id,
            br_position_id,

            -- DDHQ placeholder
            cast(null as bigint) as ddhq_race_id,

            -- Stage info
            lower(election_type) as stage_type,
            election_date,

            -- Election event name (e.g. "Illinois Primary Election")
            election_stage_name as election_name,

            -- Race-level name (e.g. "IL Franklin County Board - Full Term, District 3")
            state_code
            || ' '
            || position_name
            || case
                when sub_area_name is not null and sub_area_value is not null
                then ', ' || sub_area_name || ' ' || sub_area_value
                else ''
            end as race_name,

            -- Stage type flags
            is_primary,
            is_runoff,
            is_retention,

            -- Election details
            cast(number_of_seats as int) as number_of_seats,
            cast(null as string) as total_votes_cast,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where br_race_id is not null
        qualify
            row_number() over (
                partition by br_race_id order by candidacy_updated_at desc
            )
            = 1
    )

select
    election_stages.*,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp
from election_stages
left join
    {{ ref("int__icp_offices") }} as icp
    on election_stages.br_position_id = icp.br_database_position_id
