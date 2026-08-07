with
    source as (
        select * from {{ source("model_predictions", "llm_l2_br_match_20260126") }}
    ),
    renamed as (
        select
            name,
            id,
            br_database_id,
            state,
            -- The snapshot is not self-consistent: 365 rows carry L2's padded form.
            {{ strip_l2_district_zero_padding("l2_district_name") }}
            as l2_district_name,
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
