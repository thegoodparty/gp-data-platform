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
            -- TODO: for gp_election_id, need to roll up all election stages into a
            -- election
            -- br_race_id, -- will require matching br with ddhq data
            tbl_ddhq_matches.ddhq_race_id,
            tbl_ddhq_matches.ddhq_election_type as election_stage,
            tbl_ddhq_matches.ddhq_date as ddhq_election_stage_date,
            tbl_ddhq_matches.ddhq_race_name,
            tbl_ddhq_election_results_source.votes as total_votes_cast,
            -- TODO: add total_votes_counted, reporting_percentage
            tbl_ddhq_election_results_source._airbyte_extracted_at
        from
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        where
            1 = 1
            and tbl_ddhq_matches.ddhq_race_id is not null
            and tbl_ddhq_matches.ddhq_candidate_id is not null
            {% if is_incremental() %}
                and _airbyte_extracted_at
                > (select max(_airbyte_extracted_at) from {{ this }})
            {% endif %}
        qualify
            row_number() over (
                partition by gp_election_stage_id order by _airbyte_extracted_at desc
            )
            = 1
    )
select *
from elections
