-- One row per UTC day, agent, agent id and model. The day grain reads every
-- session, including the ones with a null `task_id`, because the daily total has
-- to equal what the swarm's ledger holds.
--
-- A cost row whose agent is missing keeps the export's `unknown_agent`
-- placeholder rather than being dropped. The export already left-joins `agents`
-- and states the placeholder, so nothing here filters: losing the row would make
-- the totals smaller than the ledger with no test able to see it.
with

    sessions as (

        select
            to_date(created_at) as usage_date,
            agent_name,
            agent_id,
            model,
            is_error,
            cost_source,
            task_id,
            cost_usd,
            coalesce(input_tokens, 0) + coalesce(output_tokens, 0) as total_tokens,
            exported_at
        from {{ ref("stg_airflow_source__delegate_swarm_session_cost") }}

    ),

    task_roots as (
        select task_id, root_task_id from {{ ref("int__delegate_swarm_task_root") }}
    ),

    sessions_with_root as (

        select sessions.*, task_roots.root_task_id
        from sessions
        left join task_roots on task_roots.task_id = sessions.task_id

    ),

    aggregated as (

        select
            usage_date,
            agent_name,
            agent_id,
            model,
            count(*) as session_count,
            -- Distinct rollup tasks, so this agrees with the task mart's
            -- denominator rather than counting the same rollup twice.
            count(distinct root_task_id) as task_count,
            count_if(is_error) as error_session_count,
            count_if(cost_source = 'unpriced') as unpriced_session_count,
            sum(total_tokens) as total_tokens,
            coalesce(sum(cost_usd), 0) as cost_usd,
            -- The `unpriced` share of `cost_usd`, so a reader can see how much
            -- of it rests on pi's own self-reported figure.
            coalesce(
                sum(case when cost_source = 'unpriced' then cost_usd else 0 end), 0
            ) as unpriced_cost_usd,
            max(exported_at) as source_exported_at
        from sessions_with_root
        group by usage_date, agent_name, agent_id, model

    ),

    -- Freshness describes the feed, so it comes from one anchor over the whole
    -- history and is constant per row. A per-row max would be the day's own
    -- stamp and would say nothing about whether the exporter is still running.
    anchor as (select max(source_exported_at) as source_loaded_at from aggregated)

select
    aggregated.usage_date,
    aggregated.agent_name,
    aggregated.agent_id,
    aggregated.model,
    aggregated.session_count,
    aggregated.task_count,
    aggregated.error_session_count,
    aggregated.unpriced_session_count,
    aggregated.total_tokens,
    aggregated.cost_usd,
    aggregated.unpriced_cost_usd,
    aggregated.source_exported_at,
    anchor.source_loaded_at,
    -- The last day the feed has fully seen: the day before the anchor's own UTC
    -- date. The exporter reads the whole ledger at 09:15, so its run on day D has
    -- everything up to D's midnight and D itself is still partial. Derived from the
    -- anchor rather than from the newest day carrying a row, because a quiet
    -- stretch has complete days with no sessions in them.
    --
    -- This is the Bifrost arm's rule, `usage_date < to_date(exported_at)`,
    -- expressed as a day so the two arms' values compare directly. Never
    -- current_date(), which would drift from the feed.
    date_sub(to_date(anchor.source_loaded_at), 1) as source_complete_through
from aggregated
cross join anchor
