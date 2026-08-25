-- The serving contract is newest-attempt-wins: the staging model must select
-- each office's maximum attempted_at, never by confidence or match state, so
-- a newer abstention supersedes an older match. Inert while the table holds
-- one run; arms the moment a second run appends. Catches an ordering edit
-- (asc for desc, or a different sort key) that would silently keep serving a
-- superseded answer while unique and the label check stay green.
with
    newest as (
        select br_database_id, max(attempted_at) as newest_attempted_at
        from {{ source("model_predictions", "llm_l2_br_match_results") }}
        group by br_database_id
    )
select s.br_database_id, s.attempted_at, n.newest_attempted_at
from {{ ref("stg_model_predictions__llm_l2_br_match") }} as s
inner join newest as n on s.br_database_id = n.br_database_id
where s.attempted_at <> n.newest_attempted_at
