{{
    config(
        materialized="table",
        tags=["intermediate", "civics", "election_stage", "archive"],
    )
}}

-- Historical archive of election stages from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Inlines m_general__election_stage logic for self-contained archive
with
    election_stages as (
        select
            {{ generate_salted_uuid(fields=["tbl_ddhq_matches.ddhq_race_id"]) }}
            as gp_election_stage_id,
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contest.contact_id as hubspot_contact_id,
            tbl_ddhq_matches.ddhq_race_id,
            tbl_ddhq_matches.ddhq_election_type as election_stage,
            tbl_ddhq_matches.ddhq_date as ddhq_election_stage_date,
            tbl_ddhq_matches.ddhq_race_name,
            tbl_ddhq_election_results_source.total_number_of_ballots_in_race
            as total_votes_cast,
            tbl_ddhq_election_results_source._airbyte_extracted_at
        from {{ ref("int__gp_ai_election_match") }} as tbl_ddhq_matches
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        left join
            {{ ref("candidacy_20260122") }} as tbl_candidacy
            on tbl_candidacy.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("int__hubspot_contest_20260122") }} as tbl_contest
            on tbl_contest.contact_id = tbl_candidacy.hubspot_contact_id
        where
            tbl_ddhq_matches.ddhq_race_id is not null
            and tbl_ddhq_matches.ddhq_candidate_id is not null
        qualify
            row_number() over (
                partition by gp_election_stage_id order by _airbyte_extracted_at desc
            )
            = 1
    ),

    -- Filter to election stages on or before 2025-12-31
    archived_election_stages as (
        select *
        from election_stages
        where
            ddhq_election_stage_date <= '2025-12-31'
            and ddhq_election_stage_date >= '1900-01-01'
    ),

    -- Only include election_stages that have a matching election in the archive
    valid_elections as (select gp_election_id from {{ ref("election_20260122") }}),

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
