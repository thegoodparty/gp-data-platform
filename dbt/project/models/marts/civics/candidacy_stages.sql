{{
    config(
        materialized="table",
        tags=["mart", "civics", "candidacy_stage"],
    )
}}

-- Candidacy Stages: One row per candidacy per election stage
-- Example: "John Smith for Seattle Mayor 2026, Primary Results"
-- A Candidacy Stage comprises a Candidacy and an Election Stage
-- Contains vendor-specific IDs and stage-specific results
with
    source as (
        select * from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
    ),

    candidacy_stages as (
        select
            {{
                generate_salted_uuid(
                    fields=["ballotready_candidacy_id"], salt="civics_candidacy_stage"
                )
            }} as gp_candidacy_stage_id,

            -- FK to candidacy
            {{
                generate_salted_uuid(
                    fields=[
                        "ballotready_candidate_id",
                        "ballotready_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_candidacy",
                )
            }} as gp_candidacy_id,

            -- FK to election stage
            {{
                generate_salted_uuid(
                    fields=["ballotready_race_id"], salt="civics_election_stage"
                )
            }} as gp_election_stage_id,

            -- BallotReady identifier
            ballotready_candidacy_id as br_candidacy_id,

            -- DDHQ placeholder
            cast(null as bigint) as ddhq_candidacy_id,

            -- Stage result
            result as candidacy_stage_result,

            -- Vote counts (NULL placeholders - future from DDHQ)
            cast(null as int) as votes_received,
            cast(null as double) as vote_percentage,

            -- HubSpot placeholder
            cast(null as bigint) as hs_contact_id,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where ballotready_candidacy_id is not null
        qualify
            row_number() over (
                partition by ballotready_candidacy_id order by candidacy_updated_at desc
            )
            = 1
    )

select *
from candidacy_stages
