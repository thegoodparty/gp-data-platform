with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_accounts") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as account_id,
            {{ adapter.quote("Name") }} as name,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("AcctNum") }} as acct_num,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("ParentRef") }} as parent_ref,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("SubAccount") }} as sub_account,
            {{ adapter.quote("AccountType") }} as account_type,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("AccountSubType") }} as account_sub_type,
            {{ adapter.quote("Classification") }} as classification,
            {{ adapter.quote("CurrentBalance") }} as current_balance,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("FullyQualifiedName") }} as fully_qualified_name,
            {{ adapter.quote("CurrentBalanceWithSubAccounts") }}
            as current_balance_with_sub_accounts

        from source
    )
select *
from renamed
