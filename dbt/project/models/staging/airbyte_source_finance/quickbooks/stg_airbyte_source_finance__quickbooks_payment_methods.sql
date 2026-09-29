with
    source as (
        select *
        from {{ source("airbyte_source_finance", "quickbooks_payment_methods") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as payment_method_id,
            {{ adapter.quote("Name") }} as name,
            {{ adapter.quote("Type") }} as type,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
