-- Gateway-computed usage per person per day. The reader caveats (not invoiced
-- cost, the overlap with the Anthropic reports, the partial newest day) are in
-- _finance__models.yml.
--
-- Model-list calls are left out. They are metadata calls, not inference: no
-- tokens and no cost, and keeping them would put a blank-model row in every
-- group. Excluding one object type rather than listing the inference ones means
-- a type the gateway adds later is counted by default.
with

    usage as (

        select
            to_date(request_at) as usage_date,
            provider,
            model,
            -- A request with neither a key nor a gateway user still happened and
            -- still cost money, so it gets a visible bucket rather than being
            -- dropped.
            coalesce(person_email, 'unattributed') as person_email,
            status,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            cached_read_tokens,
            cost_usd,
            input_cost_usd,
            output_cost_usd,
            additional_cost_usd,
            exported_at
        from {{ ref("stg_airflow_source__bifrost_request_log") }}
        where
            object_type is distinct from 'list_models'
            and status in ('success', 'error', 'cancelled')

    ),

    aggregated as (

        select
            usage_date,
            provider,
            model,
            person_email,
            count(*) as request_count,
            count_if(status = 'error') as error_count,
            -- Requests that used tokens with no gateway cost, usually a model
            -- missing from the gateway's pricing catalog. Cost is a zero there,
            -- not a null, so nothing else would show the gap.
            count_if(
                total_tokens > 0 and coalesce(cost_usd, 0) = 0
            ) as unpriced_request_count,
            sum(prompt_tokens) as prompt_tokens,
            sum(completion_tokens) as completion_tokens,
            sum(total_tokens) as total_tokens,
            sum(cached_read_tokens) as cached_read_tokens,
            -- A failed request carries a null cost and zero tokens, so zero is its
            -- true value; coalescing keeps this non-null for a group that is only
            -- failures.
            coalesce(sum(cost_usd), 0) as cost_usd,
            coalesce(sum(input_cost_usd), 0) as input_cost_usd,
            coalesce(sum(output_cost_usd), 0) as output_cost_usd,
            coalesce(sum(additional_cost_usd), 0) as additional_cost_usd,
            max(exported_at) as source_exported_at
        from usage
        group by usage_date, provider, model, person_email

    )

select *
from aggregated
