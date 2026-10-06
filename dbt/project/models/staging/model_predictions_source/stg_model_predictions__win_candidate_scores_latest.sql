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
            serving_model_id as legitimacy_model_id
        from source
    )
select *
from renamed
