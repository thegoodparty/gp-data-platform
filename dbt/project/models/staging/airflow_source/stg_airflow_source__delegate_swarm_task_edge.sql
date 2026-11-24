{{ config(materialized="ephemeral") }}

-- Ephemeral, like the cost staging beside it: the `dbt` schema is readable by
-- every data user, and only the marts should expose the swarm.
--
-- One row per task id, the newest observation of it, since each export
-- re-emits a trailing window.
with

    current_rows as (
        select *
        from {{ source("airflow_source", "delegate_swarm_task_edge") }}
        qualify
            row_number() over (partition by task_id order by exported_at desc, task_id)
            = 1
    ),

    renamed as (

        select
            task_id,
            parent_task_id,
            dispatched_by_task_id,
            -- The rollup's edge, resolved once here so the root walk and the two
            -- marts cannot disagree about which parent a task follows.
            coalesce(dispatched_by_task_id, parent_task_id) as rollup_parent_task_id,
            status,
            created_at,
            exported_at,
            loaded_at
        from current_rows

    )

select *
from renamed
