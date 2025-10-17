with
    source as (
        select *
        from {{ source("model_predictions", "candidacy_ddhq_matches_20251016") }}
    ),
    renamed as (
        select
            {{ adapter.quote("row_index") }},
            {{ adapter.quote("gp_candidacy_id") }},
            {{ adapter.quote("candidate_id") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("full_name") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("city") }},
            {{ adapter.quote("candidate_office") }},
            {{ adapter.quote("official_office_name") }},
            {{ adapter.quote("party_affiliation") }},
            {{ adapter.quote("embedding_text") }},
            {{ adapter.quote("election_date") }},
            {{ adapter.quote("election_type") }},
            {{ adapter.quote("llm_best_match") }},
            {{ adapter.quote("llm_confidence") }},
            {{ adapter.quote("llm_reasoning") }},
            {{ adapter.quote("top_10_candidates") }},
            {{ adapter.quote("has_match") }},
            {{ adapter.quote("ddhq_matched_index") }},
            {{ adapter.quote("ddhq_candidate") }},
            {{ adapter.quote("ddhq_race_name") }},
            {{ adapter.quote("ddhq_candidate_party") }},
            {{ adapter.quote("ddhq_is_winner") }},
            {{ adapter.quote("ddhq_race_id") }},
            {{ adapter.quote("ddhq_candidate_id") }},
            {{ adapter.quote("ddhq_election_type") }},
            {{ adapter.quote("ddhq_date") }},
            {{ adapter.quote("ddhq_embedding_text") }},
            {{ adapter.quote("match_similarity") }}

        from source
    )
select *
from renamed
