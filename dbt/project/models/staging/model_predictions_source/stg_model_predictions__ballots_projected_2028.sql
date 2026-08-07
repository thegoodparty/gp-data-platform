with
    source as (
        select * from {{ source("model_predictions", "ballots_projected_2028") }}
    ),
    renamed as (
        select
            {{ adapter.quote("state") }},
            {{ adapter.quote("election_year") }},
            {{ adapter.quote("election_code") }},
            {{ adapter.quote("district_type") }},
            {{ strip_l2_district_zero_padding(adapter.quote("district_name")) }}
            as district_name,
            {{ adapter.quote("ballots_projected") }},
            {{ adapter.quote("model_version") }},
            {{ adapter.quote("inference_at") }}

        from source
    )
select *
from renamed
