with
    source as (
        select * from {{ source("model_predictions", "llm_l2_br_match_20250811") }}
    ),
    renamed as (
        select
            name,
            id,
            br_database_id,
            state,
            l2_district_name,
            l2_district_type,
            is_matched,
            llm_reason,
            confidence,
            embeddings,
            top_embedding_score
        from source
    )
select *
from renamed
