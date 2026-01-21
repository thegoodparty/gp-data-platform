{{
    config(
        materialized="table",
        tags=["mart", "civics"],
    )
}}

with
    archived_election_stages as (
        -- Historical archive: election stages on or before 2025-12-31
        -- Filter out invalid dates (e.g., years like 0028, 1024 which are data entry
        -- errors)
        select *
        from {{ ref("m_general__election_stage") }}
        where
            ddhq_election_stage_date <= '2025-12-31'
            and ddhq_election_stage_date >= '1900-01-01'
    ),

    -- Only include election_stages that have a matching election in the archive
    valid_elections as (select gp_election_id from {{ ref("election") }}),

    filtered_election_stages as (
        select
            stage.gp_election_stage_id,
            stage.gp_election_id,
            stage.ddhq_race_id,
            stage.election_stage,
            stage.ddhq_election_stage_date,
            stage.ddhq_race_name,
            stage.total_votes_cast,
            stage._airbyte_extracted_at as created_at
        from archived_election_stages as stage
        inner join
            valid_elections as election
            on stage.gp_election_id = election.gp_election_id
    )

select
    gp_election_stage_id,
    gp_election_id,
    ddhq_race_id,
    election_stage,
    ddhq_election_stage_date,
    ddhq_race_name,
    total_votes_cast,
    created_at

from filtered_election_stages
