{{
    config(
        materialized="table",
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
            tbl_contacts.gp_candidacy_id,
            case
                when tbl_ddhq_matches.ddhq_race_id is not null
                then
                    {{ generate_salted_uuid(fields=["tbl_ddhq_matches.ddhq_race_id"]) }}
                else null
            end as gp_election_stage_id,
            -- as candidacy_stage_result,
            tbl_ddhq_matches.ddhq_candidate,
            tbl_ddhq_matches.ddhq_candidate_id,
            tbl_ddhq_matches.ddhq_race_id,
            tbl_ddhq_matches.ddhq_candidate_party,
            case
                when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                then true
                when tbl_ddhq_matches.ddhq_is_winner = 'N'
                then false
                else null
            end as ddhq_is_winner,
            tbl_ddhq_matches.llm_confidence as ddhq_llm_confidence,
            tbl_ddhq_matches.llm_reasoning as ddhq_llm_reasoning,
            tbl_ddhq_matches.top_10_candidates as ddhq_top_10_candidates,
            tbl_ddhq_matches.has_match as ddhq_has_match,
            tbl_ddhq_election_results_source.votes as votes_received,
            -- need to confirm logic for votes_percentage:
            -- tbl_ddhq_election_results_source.votes /
            -- tbl_ddhq_election_results_source.total_number_of_ballots_in_race as
            -- votes_percentage,
            tbl_ddhq_election_results_source.date as ddhq_election_stage_date,
            tbl_contacts.created_at,
            tbl_contacts.updated_at
        from {{ ref("int__hubspot_contacts_w_companies") }} as tbl_contacts
        left join
            {{ ref("int__gp_ai_election_match") }} as tbl_ddhq_matches
            on tbl_contacts.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    )

select *
from candidacies
