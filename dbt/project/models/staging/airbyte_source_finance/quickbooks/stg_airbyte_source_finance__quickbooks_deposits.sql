with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_deposits") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as deposit_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("CashBack") }} as cash_back,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("TotalAmt") }} as total_amt,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("PrivateNote") }} as private_note,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("DepartmentRef") }} as department_ref,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("DepositToAccountRef") }} as deposit_to_account_ref

        from source
    )
select *
from renamed
