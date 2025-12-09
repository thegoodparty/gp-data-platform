with
    source as (
        select *
        from {{ source("model_predictions", "manual_llm_election_results_20251208") }}
    ),
    renamed as (
        select
            {{ adapter.quote("hubspot_contact_id") }},
            {{ adapter.quote("manual_llm_general_election_result") }},
            {{ adapter.quote("manual_llm_election_decision_result_page") }},
            {{ adapter.quote("manual_llm_votes_received") }},
            {{ adapter.quote("manual_llm_votes_in_race") }}

        from source
    )
select *
from renamed
