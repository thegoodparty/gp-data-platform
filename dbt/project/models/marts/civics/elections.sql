{{
    config(
        materialized="table",
        tags=["mart", "civics", "election"],
    )
}}

-- Elections: One row per position + year (full election cycle)
-- Example: "Seattle Mayor 2026" = 1 row encompassing primary, general, and any runoffs
with
    source as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    elections as (
        select
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
            ballotready_position_id as br_position_id,

            -- Election info
            year(election_date) as election_year,

            -- Position/Office info
            position_name,
            normalized_position_name,
            candidate_office,
            office_level,
            office_level_formatted,

            -- Geographic info
            state_code,
            state_name,
            city,
            sub_area_name,
            sub_area_value,
            mtfcc,

            -- Election details
            cast(number_of_seats as int) as number_of_seats,
            is_judicial,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where ballotready_position_id is not null
        qualify
            row_number() over (
                partition by ballotready_position_id, year(election_date)
                order by candidacy_updated_at desc
            )
            = 1
    )

select *
from elections
