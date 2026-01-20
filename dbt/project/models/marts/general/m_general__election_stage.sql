{{
    config(
        materialized="incremental",
        unique_key="gp_election_stage_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "election_stage"],
    )
}}

with
    elections as (
        select
            {{ generate_salted_uuid(fields=["tbl_ddhq_matches.ddhq_race_id"]) }}
            as gp_election_stage_id,
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contest.contact_id as hubspot_contact_id,
            -- br_race_id, -- will require matching br with ddhq data
            tbl_ddhq_matches.ddhq_race_id,
            tbl_ddhq_matches.ddhq_election_type as election_stage,
            tbl_ddhq_matches.ddhq_date as ddhq_election_stage_date,
            tbl_ddhq_matches.ddhq_race_name,
            tbl_ddhq_election_results_source.total_number_of_ballots_in_race
            as total_votes_cast,
            -- TODO: add total_votes_counted, reporting_percentage
            tbl_ddhq_election_results_source._airbyte_extracted_at
        from {{ ref("int__gp_ai_election_match") }} as tbl_ddhq_matches
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        left join
            {{ ref("m_general__candidacy_v2") }} as tbl_candidacy
            on tbl_candidacy.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("int__hubspot_contest") }} as tbl_contest
            on tbl_contest.contact_id = tbl_candidacy.hubspot_contact_id
        where
            1 = 1
            and tbl_ddhq_matches.ddhq_race_id is not null
            and tbl_ddhq_matches.ddhq_candidate_id is not null
            {% if is_incremental() %}
                and tbl_ddhq_election_results_source._airbyte_extracted_at
                >= (select max(_airbyte_extracted_at) from {{ this }})
            {% endif %}
        qualify
            row_number() over (
                partition by gp_election_stage_id order by _airbyte_extracted_at desc
            )
            = 1
    ),
    -- Only include election_stages that have a matching election
    valid_elections as (select gp_election_id from {{ ref("m_general__election") }}),

    elections_with_gp_election_id as (
        select elections.*
        from elections
        inner join
            valid_elections on elections.gp_election_id = valid_elections.gp_election_id
    )

select *
from elections_with_gp_election_id
