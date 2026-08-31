-- ICP must not consume the supervised backlog run's matches below confidence
-- 90, nor that run's matches on baseline-abstained offices (the stale
-- re-match mechanism, quarantined from publication). The fence lives in
-- int__icp_offices's l2_match CTE and is scoped to that run's key; a fenced
-- office carries a null match there. Any row returned here means the fence
-- was removed or bypassed.
with
    fenced as (
        select stg.br_database_id
        from {{ ref("stg_model_predictions__llm_l2_br_match") }} as stg
        left join
            {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as baseline
            on baseline.br_database_id = stg.br_database_id
            and baseline.l2_district_name is null
        where
            stg.l2_district_name is not null
            and stg.attempted_at = timestamp '2026-08-31 19:46:39'
            and (stg.confidence < 90 or baseline.br_database_id is not null)
    )

select icp.br_database_position_id
from {{ ref("int__icp_offices") }} as icp
inner join fenced on fenced.br_database_id = icp.br_database_position_id
where icp.l2_district_name is not null
