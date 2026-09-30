with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_bills") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as bill_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("Balance") }} as balance,
            {{ adapter.quote("DueDate") }} as due_date,
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("TotalAmt") }} as total_amt,
            {{ adapter.quote("DocNumber") }} as doc_number,
            {{ adapter.quote("LinkedTxn") }} as linked_txn,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("VendorRef") }} as vendor_ref,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("PrivateNote") }} as private_note,
            {{ adapter.quote("APAccountRef") }} as ap_account_ref,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("SalesTermRef") }} as sales_term_ref,
            {{ adapter.quote("DepartmentRef") }} as department_ref,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
