{{
    config(
        materialized="incremental",
        unique_key="gp_candidacy_stage_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["mart", "general", "candidacy", "stage"],
    )
}}

with
    candidacies as (
        select
            -- Identifiers
            {{
                generate_salted_uuid(
                    fields=[
                        "tbl_contacts.gp_candidacy_id",
                        "tbl_ddhq_matches.ddhq_race_id",
                    ]
                )
            }} as gp_candidacy_stage_id,
            -- br_candidacy_id, -- need to match ddhq with br for this
            -- ddhq_candidacy_id, -- maybe some combination of `ddhq_candidate_id` and
            -- `ddhq_race_id`
            tbl_contacts.gp_candidacy_id,
            -- tbl_ddhq_matches.gp_election_stage_id,
            -- as candidacy_stage_result,
            tbl_ddhq_matches.ddhq_candidate,
            tbl_ddhq_matches.ddhq_candidate_party,
            tbl_ddhq_matches.ddhq_is_winner,
            tbl_ddhq_matches.llm_confidence as ddhq_llm_confidence,
            tbl_ddhq_matches.llm_reasoning as ddhq_llm_reasoning,
            tbl_ddhq_matches.top_10_candidates as ddhq_top_10_candidates,
            tbl_ddhq_matches.has_match as ddhq_has_match,
            tbl_ddhq_election_results_source.votes as votes_received,
            -- votes_percentage
            tbl_contacts.created_at,
            tbl_contacts.updated_at
        from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_contacts
        left join
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
            on tbl_contacts.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        {% if is_incremental() %}
            where tbl_contacts.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    )

select *
from candidacies
