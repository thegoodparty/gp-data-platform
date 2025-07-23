with
    source as (
        select *
        from
            {{ source("model_predictions", "turnout_projections_even_years_20250709") }}
    ),
    renamed as (
        select
            {{ adapter.quote("state") }},
            {{ adapter.quote("election_year") }},
            {{ adapter.quote("election_code") }},
            {{ adapter.quote("district_type") }},
            {{ adapter.quote("district_name") }},
            {{ adapter.quote("ballots_projected") }},
            {{ adapter.quote("model_version") }},
            {{ adapter.quote("inference_at") }}

        from source
    )
select *
from renamed
