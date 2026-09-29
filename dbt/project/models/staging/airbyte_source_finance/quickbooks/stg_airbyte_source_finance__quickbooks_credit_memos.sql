with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_credit_memos") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as credit_memo_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("Balance") }} as balance,
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("BillAddr") }} as bill_addr,
            {{ adapter.quote("ClassRef") }} as class_ref,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("ShipAddr") }} as ship_addr,
            {{ adapter.quote("TotalAmt") }} as total_amt,
            {{ adapter.quote("BillEmail") }} as bill_email,
            {{ adapter.quote("DocNumber") }} as doc_number,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("CustomField") }} as custom_field,
            {{ adapter.quote("CustomerRef") }} as customer_ref,
            {{ adapter.quote("EmailStatus") }} as email_status,
            {{ adapter.quote("PrintStatus") }} as print_status,
            {{ adapter.quote("CustomerMemo") }} as customer_memo,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("HomeTotalAmt") }} as home_total_amt,
            {{ adapter.quote("SalesTermRef") }} as sales_term_ref,
            {{ adapter.quote("TxnTaxDetail") }} as txn_tax_detail,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("RemainingCredit") }} as remaining_credit,
            {{ adapter.quote("ApplyTaxAfterDiscount") }} as apply_tax_after_discount

        from source
    )
select *
from renamed
