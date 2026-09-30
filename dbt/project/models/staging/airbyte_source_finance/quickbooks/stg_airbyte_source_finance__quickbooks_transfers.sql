with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_transfers") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as transfer_id,
            {{ adapter.quote("Amount") }} as amount,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("PrivateNote") }} as private_note,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("ToAccountRef") }} as to_account_ref,
            {{ adapter.quote("FromAccountRef") }} as from_account_ref,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
