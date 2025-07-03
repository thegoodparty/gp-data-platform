with
    source as (
        select * from {{ source("model_predictions", "turnout_projections_model2odd") }}
    ),
    renamed as (
        select
            ballots_projected,
            state,
            office_type,
            office_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from source
    )
select *
from renamed
