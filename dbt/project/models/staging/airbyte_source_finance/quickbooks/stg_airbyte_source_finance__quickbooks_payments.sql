with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_payments") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as payment_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("TotalAmt") }} as total_amt,
            {{ adapter.quote("LinkedTxn") }} as linked_txn,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("CustomerRef") }} as customer_ref,
            {{ adapter.quote("PrivateNote") }} as private_note,
            {{ adapter.quote("ARAccountRef") }} as ar_account_ref,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("UnappliedAmt") }} as unapplied_amt,
            {{ adapter.quote("PaymentRefNum") }} as payment_ref_num,
            {{ adapter.quote("ProcessPayment") }} as process_payment,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("PaymentMethodRef") }} as payment_method_ref,
            {{ adapter.quote("DepositToAccountRef") }} as deposit_to_account_ref

        from source
    )
select *
from renamed
