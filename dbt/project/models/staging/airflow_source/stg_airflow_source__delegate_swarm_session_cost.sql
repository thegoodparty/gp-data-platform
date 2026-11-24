{{ config(materialized="ephemeral") }}

-- Ephemeral: this is a per-session trace of what the swarm ran, and the `dbt`
-- schema grants SELECT to every data user.
--
-- One row per ledger row id, the newest observation of it, since each export
-- re-emits a trailing window. `exported_at` orders the two because `loaded_at`
-- is the same for every row of one file.
with

    current_rows as (
        select *
        from {{ source("airflow_source", "delegate_swarm_session_cost") }}
        qualify row_number() over (partition by id order by exported_at desc, id) = 1
    ),

    renamed as (

        select
            id,
            session_id,
            task_id,
            agent_id,
            -- The ledger's join to `agents` already substitutes its own
            -- placeholder, so a blank here is a contract break rather than an
            -- expected gap. `unknown_agent` is the stated value the mart
            -- documents, so it is not nulled.
            agent_name,
            model,
            cost_source,
            kind,
            total_cost_usd as cost_usd,
            harness_cost_usd,
            input_tokens,
            output_tokens,
            cache_read_tokens,
            cache_write_tokens,
            cache_write_5m_tokens,
            cache_write_1h_tokens,
            reasoning_output_tokens,
            thinking_tokens,
            duration_ms,
            num_turns,
            -- The ledger's 0/1 integer becomes a boolean once, here, so no mart
            -- has to remember which shape it is.
            coalesce(is_error, 0) != 0 as is_error,
            created_at,
            exported_at,
            loaded_at

        from current_rows

    )

select *
from renamed
