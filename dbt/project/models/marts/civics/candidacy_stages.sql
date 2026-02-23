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
                    fields=["br_candidacy_id"], salt="civics_candidacy_stage"
                )
            }} as gp_candidacy_stage_id,

            -- FK to candidacy
            {{
                generate_salted_uuid(
                    fields=[
                        "br_candidate_id",
                        "br_position_id",
                        "cast(year(election_date) as string)",
                    ],
                    salt="civics_candidacy",
                )
            }} as gp_candidacy_id,

            -- FK to election stage
            {{
                generate_salted_uuid(
                    fields=["br_race_id"], salt="civics_election_stage"
                )
            }} as gp_election_stage_id,

            -- BallotReady identifier
            br_candidacy_id,

            -- DDHQ placeholder
            cast(null as bigint) as ddhq_candidacy_id,

            -- Stage result
            result as candidacy_stage_result,

            -- Vote counts (NULL placeholders - future from DDHQ)
            cast(null as int) as votes_received,
            cast(null as double) as vote_percentage,

            -- HubSpot placeholder
            cast(null as bigint) as hs_contact_id,

            -- Position ID for ICP join
            br_position_id,

            -- Timestamps
            candidacy_created_at as created_at,
            candidacy_updated_at as updated_at

        from source
        where br_candidacy_id is not null
        qualify
            row_number() over (
                partition by br_candidacy_id order by candidacy_updated_at desc
            )
            = 1
    )

select
    candidacy_stages.gp_candidacy_stage_id,
    candidacy_stages.gp_candidacy_id,
    candidacy_stages.gp_election_stage_id,
    candidacy_stages.br_candidacy_id,
    candidacy_stages.ddhq_candidacy_id,
    candidacy_stages.candidacy_stage_result,
    candidacy_stages.votes_received,
    candidacy_stages.vote_percentage,
    candidacy_stages.hs_contact_id,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    candidacy_stages.created_at,
    candidacy_stages.updated_at
from candidacy_stages
left join
    {{ ref("int__icp_offices") }} as icp
    on candidacy_stages.br_position_id = icp.br_database_position_id
