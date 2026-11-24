-- Every AI spend source in one table, so a dashboard reads one model instead of
-- one per vendor. Each arm keeps its own basis, because the numbers are not
-- comparable across sources without saying who measured them.
with

    bifrost as (

        select
            usage_date,
            'bifrost' as source,
            provider,
            model,
            case
                when person_email = 'unattributed' then 'unattributed' else 'person'
            end as attribution_kind,
            -- No lower() here: staging already lower-cased both identity inputs
            -- and _finance__models.yml tests the column, so it would be a no-op.
            person_email as attribution,
            -- Both marts carry a decimal cost, but their precisions differ and a
            -- union would settle on the wider one.
            cast(cost_usd as decimal(20, 6)) as cost_usd,
            request_count,
            cast(null as bigint) as session_count,
            total_tokens,
            'gateway_computed' as cost_basis,
            source_exported_at
        from {{ ref("bifrost_daily_usage") }}

    ),

    -- A day is complete only once an export has passed its midnight, never
    -- against current_date(), because the newest day is still partial.
    bifrost_anchor as (select max(source_exported_at) as source_loaded_at from bifrost),

    bifrost_complete_through as (

        select max(bifrost.usage_date) as source_complete_through
        from bifrost
        cross join bifrost_anchor
        where bifrost.usage_date < to_date(bifrost_anchor.source_loaded_at)

    ),

    bifrost_arm as (

        select
            bifrost.*,
            bifrost_anchor.source_loaded_at,
            bifrost_complete_through.source_complete_through
        from bifrost
        cross join bifrost_anchor
        cross join bifrost_complete_through

    ),

    anthropic as (

        select
            usage_date,
            'anthropic' as source,
            'anthropic' as provider,
            -- The vendor does not split its cost by model at the grain we
            -- ingest, so any per-model Claude figure would be invented. The
            -- label says that and still sorts with the real model ids.
            'claude_all_models' as model,
            case
                when is_unattributed then 'unattributed' else 'person'
            end as attribution_kind,
            person_email as attribution,
            cast(cost_usd as decimal(20, 6)) as cost_usd,
            request_count,
            cast(null as bigint) as session_count,
            cast(null as bigint) as total_tokens,
            'vendor_reported' as cost_basis,
            source_loaded_at,
            source_complete_through
        from {{ ref("anthropic_daily_usage") }}

    ),

    -- The swarm is a third arm, but not a third kind of number: it reports its
    -- own session costs, so it carries `self_reported`. It has no person at all,
    -- so `attribution` is the agent's operator-set label and never a synthesised
    -- email. `not-null` on `attribution` therefore holds without a bucket row.
    delegate_swarm as (

        select
            usage_date,
            'delegate_swarm' as source,
            -- The swarm calls Fireworks directly, through pi's own provider id.
            'fireworks' as provider,
            model,
            'agent' as attribution_kind,
            agent_name as attribution,
            cast(cost_usd as decimal(20, 6)) as cost_usd,
            cast(null as bigint) as request_count,
            cast(session_count as bigint) as session_count,
            cast(total_tokens as bigint) as total_tokens,
            'self_reported' as cost_basis,
            source_loaded_at,
            source_complete_through
        from {{ ref("delegate_swarm_daily_usage") }}

    )

select
    usage_date,
    source,
    provider,
    model,
    attribution_kind,
    attribution,
    cost_usd,
    request_count,
    session_count,
    total_tokens,
    cost_basis,
    source_loaded_at,
    source_complete_through
from bifrost_arm

union all

select
    usage_date,
    source,
    provider,
    model,
    attribution_kind,
    attribution,
    cost_usd,
    request_count,
    session_count,
    total_tokens,
    cost_basis,
    source_loaded_at,
    source_complete_through
from anthropic

union all

select
    usage_date,
    source,
    provider,
    model,
    attribution_kind,
    attribution,
    cost_usd,
    request_count,
    session_count,
    total_tokens,
    cost_basis,
    source_loaded_at,
    source_complete_through
from delegate_swarm
