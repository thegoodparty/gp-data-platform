with
    source as (select * from {{ source("model_predictions", "viability_scores") }}),
    renamed as (select id, viability_rating_2_0, viability_rating_automated from source)
select *
from renamed
