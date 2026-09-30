with
    source as (
        select *
        from {{ source("airbyte_source_finance", "quickbooks_journal_entries") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as journal_entry_id,
            {{ adapter.quote("Line") }} as line,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("TxnDate") }} as txn_date,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("DocNumber") }} as doc_number,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("Adjustment") }} as adjustment,
            {{ adapter.quote("TaxRateRef") }} as tax_rate_ref,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("PrivateNote") }} as private_note,
            {{ adapter.quote("ExchangeRate") }} as exchange_rate,
            {{ adapter.quote("TxnTaxDetail") }} as txn_tax_detail,
            {{ adapter.quote("airbyte_cursor") }}

        from source
    )
select *
from renamed
