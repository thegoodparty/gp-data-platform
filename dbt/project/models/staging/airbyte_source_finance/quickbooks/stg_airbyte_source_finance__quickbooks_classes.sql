with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_classes") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as class_id,
            {{ adapter.quote("Name") }} as name,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("SubClass") }} as sub_class,
            {{ adapter.quote("ParentRef") }} as parent_ref,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("FullyQualifiedName") }} as fully_qualified_name

        from source
    )
select *
from renamed
