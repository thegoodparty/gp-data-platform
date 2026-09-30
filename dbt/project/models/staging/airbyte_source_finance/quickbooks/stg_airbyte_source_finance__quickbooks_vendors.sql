with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_vendors") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as vendor_id,
            {{ adapter.quote("Fax") }} as fax,
            {{ adapter.quote("Title") }} as title,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("Mobile") }} as mobile,
            {{ adapter.quote("Suffix") }} as suffix,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("AcctNum") }} as acct_num,
            {{ adapter.quote("Balance") }} as balance,
            {{ adapter.quote("TermRef") }} as term_ref,
            {{ adapter.quote("WebAddr") }} as web_addr,
            {{ adapter.quote("BillAddr") }} as bill_addr,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("GivenName") }} as given_name,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("FamilyName") }} as family_name,
            {{ adapter.quote("MiddleName") }} as middle_name,
            {{ adapter.quote("Vendor1099") }} as vendor1099,
            {{ adapter.quote("CompanyName") }} as company_name,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("DisplayName") }} as display_name,
            {{ adapter.quote("PrimaryPhone") }} as primary_phone,
            {{ adapter.quote("TaxIdentifier") }} as tax_identifier,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("PrimaryEmailAddr") }} as primary_email_addr,
            {{ adapter.quote("PrintOnCheckName") }} as print_on_check_name

        from source
    )
select *
from renamed
