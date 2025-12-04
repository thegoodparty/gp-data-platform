with
    source as (
        select * from {{ source("model_predictions", "candidacy_br_matches_20251204") }}
    ),
    renamed as (
        select
            {{ adapter.quote("candidate_idx") }},
            {{ adapter.quote("candidate_code") }},
            {{ adapter.quote("matched_code") }},
            {{ adapter.quote("hubspot_contact_id") }},
            {{ adapter.quote("hubspot_company_id") }},
            {{ adapter.quote("match_score") }},
            {{ adapter.quote("match_type") }},
            {{ adapter.quote("human_match_status") }},
            {{ adapter.quote("final_match_status") }},
            {{ adapter.quote("final_election_result") }},
            {{ adapter.quote("br_id") }},
            {{ adapter.quote("br_is_runoff") }},
            {{ adapter.quote("br_candidacy_updated_at") }}

        from source
    )
select *
from renamed
