with
    source as (
        select *
        from {{ source("model_predictions", "candidacy_ddhq_matches_20250826") }}
    ),
    renamed as (
        select
            gp_candidacy_id,
            candidate_id,
            first_name,
            last_name,
            full_name,
            state,
            city,
            candidate_office,
            official_office_name,
            party_affiliation,
            embedding_text,
            election_date,
            election_type,
            llm_best_match,
            llm_confidence,
            llm_reasoning,
            top_10_candidates,
            has_match,
            ddhq_matched_index,
            ddhq_candidate,
            ddhq_race_name,
            ddhq_candidate_party,
            ddhq_is_winner,
            ddhq_race_id,
            ddhq_candidate_id,
            ddhq_election_type,
            ddhq_date,
            ddhq_embedding_text,
            match_similarity
        from source
    )
select *
from renamed
