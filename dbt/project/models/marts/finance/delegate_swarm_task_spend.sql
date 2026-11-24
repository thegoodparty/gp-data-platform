-- One row per rollup task, and the single place the cost-per-task definition
-- lives. Changing what counts toward a task is a one-model edit, and the unit
-- tests in `_finance__models.yml` pin the current behaviour so a change is
-- deliberate.
--
-- What counts, and why:
--
-- - a task's spend is the sum over every session whose `task_id` rolls up to
-- it, itself included, where the rollup edge is
-- coalesce(dispatched_by_task_id, parent_task_id) walked to its root;
-- - failed attempts count. A session with `is_error` and a task in a failed
-- state are both real spend, and excluding them would make a retried task
-- look cheaper than one that succeeded first time;
-- - `kind = 'system-one'` rows count, because upstream's own task totals sum
-- every row;
-- - `cost_source = 'unpriced'` dollars count in every total, with
-- `unpriced_cost_usd` beside them so a reader can see the share that rests on
-- pi's self-reported figure;
-- - a session with a null `task_id` is excluded from this grain. It has no task
-- to roll up to, and it stays in the daily mart, so nothing is lost overall.
with

    task_roots as (
        select task_id, root_task_id from {{ ref("int__delegate_swarm_task_root") }}
    ),

    -- The row grain is every rollup task the landing knows about, not only the
    -- ones with sessions: a rollup that spent nothing is a real answer, and
    -- `child_task_count` has to describe the tree rather than the traffic so a
    -- broken chain shows up as a smaller number.
    subtrees as (

        select
            root_task_id,
            -- The subtree's size, the root included. A leaf is 1.
            count(distinct task_id) as child_task_count
        from task_roots
        group by root_task_id

    ),

    sessions as (

        select task_id, cost_source, cost_usd, created_at
        from {{ ref("stg_airflow_source__delegate_swarm_session_cost") }}
        where task_id is not null

    ),

    attributed as (

        select
            task_roots.root_task_id,
            sessions.cost_source,
            sessions.cost_usd,
            sessions.created_at
        from sessions
        inner join task_roots on task_roots.task_id = sessions.task_id

    ),

    spent as (

        select
            root_task_id,
            count(*) as session_count,
            coalesce(sum(cost_usd), 0) as cost_usd,
            coalesce(
                sum(case when cost_source = 'unpriced' then cost_usd else 0 end), 0
            ) as unpriced_cost_usd,
            min(created_at) as first_session_at,
            max(created_at) as last_session_at
        from attributed
        group by root_task_id

    )

select
    subtrees.root_task_id as rollup_task_id,
    coalesce(spent.session_count, 0) as session_count,
    subtrees.child_task_count,
    coalesce(spent.cost_usd, 0) as cost_usd,
    coalesce(spent.unpriced_cost_usd, 0) as unpriced_cost_usd,
    spent.first_session_at,
    spent.last_session_at
from subtrees
left join spent on spent.root_task_id = subtrees.root_task_id
