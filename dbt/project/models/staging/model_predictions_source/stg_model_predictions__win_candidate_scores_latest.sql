with
    source as (
        select * from {{ source("model_predictions", "win_candidate_scores_latest") }}
    ),
    renamed as (
        select
            campaign_id,
            user_id,
            scored_at,
            -- The serving tier, built on every feature. The profile tier is the
            -- restricted model for analysis about the scored group, so it is not
            -- carried here.
            serving_expected_rate as legitimacy_score,
            serving_bucket as legitimacy_bucket,
            -- The band in words, for readers who are not reading the code. The
            -- number prefix makes text order match score order.
            case
                serving_bucket
                when 'p_under_5'
                then '1. [0-5%] very unlikely'
                when 'p_5_15'
                then '2. [5-15%] unlikely'
                when 'p_15_35'
                then '3. [15-35%] possible'
                when 'p_35_60'
                then '4. [35-60%] toss-up'
                when 'p_60_plus'
                then '5. [60-100%] likely'
            end as legitimacy_label,
            serving_model_id as legitimacy_model_id
        from source
    )
select *
from renamed
