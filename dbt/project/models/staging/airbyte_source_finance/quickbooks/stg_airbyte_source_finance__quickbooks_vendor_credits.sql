with
    source as (
        select *
        from {{ source("airbyte_source_finance", "quickbooks_vendor_credits") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as vendor_credit_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("TotalAmt") }} as total_amt,
            {{ adapter.quote("DocNumber") }} as doc_number,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("VendorRef") }} as vendor_ref,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("APAccountRef") }} as ap_account_ref,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("DepartmentRef") }} as department_ref,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
