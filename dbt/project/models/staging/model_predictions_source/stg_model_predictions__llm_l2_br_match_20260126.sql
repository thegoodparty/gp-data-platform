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
            -- The snapshot also spells "no match" as the literal NOT_MATCHED in both
            -- district columns instead of null, which makes a `district_name is not
            -- null` filter read ~12k unmatched positions as matched: the strings pass
            -- not-null checks, look like data, and join to nothing. Null them so
            -- is_matched is the only predicate for match state. The two-way contract
            -- is pinned by assert_l2_br_match_unmatched_rows_have_null_district.
            nullif(ltrim('0', l2_district_name), 'NOT_MATCHED') as l2_district_name,
            nullif(l2_district_type, 'NOT_MATCHED') as l2_district_type,
            is_matched,
            llm_reason,
            confidence,
            embeddings,
            top_embedding_score
        from source
    )
select *
from renamed
