with
    source as (
        select * from {{ source("model_predictions", "voter_turnout_scores_20260730") }}
    ),
    renamed as (
        select
            {{ adapter.quote("LALVOTERID") }},
            {{ adapter.quote("prob_vote") }},
            {{ adapter.quote("prediction") }},
            {{ adapter.quote("election_year") }},
            {{ adapter.quote("election_code") }},
            {{ adapter.quote("model_version") }},
            {{ adapter.quote("state") }}

        from source
    )
select *
from renamed
