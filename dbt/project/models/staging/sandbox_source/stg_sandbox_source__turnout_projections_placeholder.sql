with
    source as (
        select * from {{ source("sandbox_source", "turnout_projections_placeholder") }}
    ),
    renamed as (
        select
            {{ adapter.quote("OfficeType") }},
            {{ adapter.quote("OfficeName") }},
            {{ adapter.quote("ballots_projected") }},
            {{ adapter.quote("State") }},
            {{ adapter.quote("Election_Type") }},
            {{ adapter.quote("Election_Year") }},
            {{ adapter.quote("Model_version") }},
            {{ adapter.quote("inference_date") }}

        from source
    )
select *
from renamed
