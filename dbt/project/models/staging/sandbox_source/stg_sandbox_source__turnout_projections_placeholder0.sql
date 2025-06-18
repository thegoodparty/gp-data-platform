with
    source as (
        select * from {{ source("sandbox_source", "turnout_projections_placeholder0") }}
    ),
    renamed as (
        select
            {{ adapter.quote("ballots_projected") }},
            {{ adapter.quote("inference_date") }} as inference_at,
            {{ adapter.quote("election_year") }},
            {{ adapter.quote("election_code") }},
            {{ adapter.quote("model_version") }},
            {{ adapter.quote("state") }},
            {{ adapter.quote("officeType") }} as office_type,
            {{ adapter.quote("officeName") }} as office_name

        from source
    )
select *
from renamed
