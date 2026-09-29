with
    source as (
        select *
        from {{ source("airbyte_source_finance", "quickbooks_time_activities") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as time_activity_id,
            {{ adapter.quote("Hours") }} as hours,
            {{ adapter.quote("NameOf") }} as name_of,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("ItemRef") }} as item_ref,
            {{ adapter.quote("Minutes") }} as minutes,
            {{ adapter.quote("Taxable") }} as taxable,
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("HourlyRate") }} as hourly_rate,
            {{ adapter.quote("CustomerRef") }} as customer_ref,
            {{ adapter.quote("Description") }} as description,
            {{ adapter.quote("EmployeeRef") }} as employee_ref,
            {{ adapter.quote("BillableStatus") }} as billable_status,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
