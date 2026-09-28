with

    source as (
        select * from {{ source("airbyte_source", "anthropic_api_usage_report") }}
    ),

    -- one API record per daily bucket; results carries one entry per product and model
    exploded as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            starting_at,
            ending_at,
            explode(
                from_json(
                    results,
                    'array<struct<product:string,model:string,'
                    || 'uncached_input_tokens:bigint,cache_read_input_tokens:bigint,'
                    || 'cache_creation:struct<ephemeral_5m_input_tokens:bigint,'
                    || 'ephemeral_1h_input_tokens:bigint>,'
                    || 'output_tokens:bigint,requests:bigint,'
                    || 'server_tool_use:struct<web_search_requests:bigint>>>'
                )
            ) as result

        from source

    ),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(starting_at as timestamp) as bucket_start,
            cast(ending_at as timestamp) as bucket_end,
            result.product,
            result.model,
            result.uncached_input_tokens,
            result.cache_read_input_tokens,
            result.cache_creation.ephemeral_5m_input_tokens
            as cache_creation_5m_input_tokens,
            result.cache_creation.ephemeral_1h_input_tokens
            as cache_creation_1h_input_tokens,
            result.output_tokens,
            result.requests,
            result.server_tool_use.web_search_requests as web_search_requests

        from exploded

    )

select *
from renamed
