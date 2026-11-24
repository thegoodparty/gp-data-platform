{{ config(materialized="ephemeral") }}

-- The one place the rollup walk lives. Both `delegate_swarm_daily_usage` and
-- `delegate_swarm_task_spend` read this, so the two cannot disagree about which
-- task a session's spend rolls up to, and `task_count` and `child_task_count`
-- cannot drift apart.
--
-- A task rolls up through `rollup_parent_task_id`, which staging resolved as
-- coalesce(dispatched_by_task_id, parent_task_id). Three things bound the walk:
--
-- - a parent with no row of its own is never stepped to, so the task whose
-- parent is missing is its own root. The export's window and the append-only
-- landing can both leave an ancestor out, and a missing ancestor then costs a
-- level of rollup rather than a session's spend;
-- - the depth bound, which keeps a parent cycle upstream from hanging the
-- nightly build. A cycle resolves to a task inside the loop: arbitrary, but
-- deterministic, and the same answer on every run and for both readers. A task
-- that feeds *into* a cycle can settle on a different member of the loop than the
-- loop's own tasks do, so one connected component can appear under two roots. It
-- is bounded and it costs rollup, never spend, and it is the residue accepted for
-- a state upstream should not produce;
-- - a null parent, which is a root.
with recursive

    parents as (

        select task_id, rollup_parent_task_id
        from {{ ref("stg_airflow_source__delegate_swarm_task_edge") }}

    ),

    walk as (

        select task_id, task_id as ancestor_task_id, 0 as depth
        from parents

        union all

        -- Joining `parent` back to `parents` is what enforces "only step to an
        -- ancestor that landed", rather than stepping to an id the edge table
        -- merely names.
        select walk.task_id, parent.task_id, walk.depth + 1
        from walk
        inner join parents as here on here.task_id = walk.ancestor_task_id
        inner join parents as parent on parent.task_id = here.rollup_parent_task_id
        where walk.depth < 32

    ),

    deepest as (

        select task_id, ancestor_task_id as root_task_id, depth
        from walk
        -- One row per task per depth, because a task has one outgoing edge and each
        -- step joins a deduped key, so the deepest row is already unique.
        qualify row_number() over (partition by task_id order by depth desc) = 1

    )

select task_id, root_task_id, depth
from deepest
