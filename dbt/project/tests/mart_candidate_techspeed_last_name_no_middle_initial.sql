-- A2: TS-side last_name parser assertion at the int__civics_candidate_techspeed
-- layer. Matches the A1 staging-layer bug-pattern set plus a trailing-comma
-- guard, but checks the intermediate model directly rather than the mart so
-- the test cleanly scopes to "did the TS parser produce a clean last_name?".
--
-- Why not the mart: the mart's consolidated last_name can come from any
-- source (BR / DDHQ / TS / gp_api) via cross-source precedence. A bad
-- last_name in the mart may originate from a non-TS source (e.g. gp_api's
-- 'S Price' surfacing for Paul Price because the mart precedence picks
-- gp_api over BR + DDHQ + TS in some rows), which is a separate
-- cross-source consolidation concern that doesn't belong to a TS-parser
-- test. A follow-up source-agnostic mart-level test should cover that.
--
-- The filename keeps its mart_ prefix for git history continuity; the
-- effective scope is the TS intermediate.
select gp_candidate_id, last_name
from {{ ref("int__civics_candidate_techspeed") }}
where
    last_name is not null
    and (
        last_name rlike '^[A-Z][.] ?[A-Za-z]'
        or last_name rlike '^[A-Z] [A-Za-z]'
        or last_name rlike '[A-Za-z] [A-Z]$'
        or last_name rlike ',$'
    )
