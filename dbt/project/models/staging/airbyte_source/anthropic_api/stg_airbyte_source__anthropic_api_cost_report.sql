with

    source as (
        select * from {{ source("airbyte_source", "anthropic_api_cost_report") }}
    ),

    -- one API record per daily bucket; results carries one entry per product,
    -- model, cost_type and token_type
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
                    'array<struct<product:string,model:string,cost_type:string,'
                    || 'token_type:string,amount:string,list_amount:string,'
                    || 'currency:string,requests:bigint>>'
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
            result.cost_type,
            result.token_type,
            -- the API returns fractional cents as decimal strings ("41280.000000" is
            -- $412.80); parsed as decimal, never double, and converted once here
            cast(
                cast(result.amount as decimal(20, 6)) / 100 as decimal(20, 6)
            ) as amount,
            cast(
                cast(result.list_amount as decimal(20, 6)) / 100 as decimal(20, 6)
            ) as list_amount,
            result.currency,
            result.requests

        from exploded

    )

select *
from renamed
