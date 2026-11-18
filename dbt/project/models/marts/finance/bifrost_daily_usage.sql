-- Reader caveats live in _finance__models.yml: the cost is the gateway's own,
-- and the newest day is partial.
--
-- Model-list calls are excluded by type, not by an inclusion list, so a new
-- inference type counts by default. They carry no tokens or cost and would add
-- a blank-model row to every group.
with

    usage as (

        select
            to_date(request_at) as usage_date,
            provider,
            model,
            -- Key-less requests still cost money, so they get a bucket rather
            -- than a null that fails the not-null test.
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
            -- Tokens with no gateway cost, usually a model missing from the
            -- pricing catalog. Cost is 0 there, not null, so nothing else
            -- surfaces it.
            count_if(
                total_tokens > 0 and coalesce(cost_usd, 0) = 0
            ) as unpriced_request_count,
            sum(prompt_tokens) as prompt_tokens,
            sum(completion_tokens) as completion_tokens,
            sum(total_tokens) as total_tokens,
            sum(cached_read_tokens) as cached_read_tokens,
            -- A null cost always comes with zero tokens, so 0 is the true
            -- value and this stays non-null for a group of only failures.
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
