with

    source as (
        select * from {{ source("airbyte_source", "anthropic_api_user_cost_report") }}
    ),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            cast(starting_at as timestamp) as bucket_start,
            cast(ending_at as timestamp) as bucket_end,
            user_id,
            actor:email::string as user_email,
            actor:name::string as user_name,
            actor:deleted::boolean as is_user_deleted,
            -- fractional cents as decimal strings; converted to dollars once here
            cast(cast(amount as decimal(20, 6)) / 100 as decimal(20, 6)) as amount,
            cast(
                cast(list_amount as decimal(20, 6)) / 100 as decimal(20, 6)
            ) as list_amount,
            currency,
            requests

        from source

    )

select *
from renamed
