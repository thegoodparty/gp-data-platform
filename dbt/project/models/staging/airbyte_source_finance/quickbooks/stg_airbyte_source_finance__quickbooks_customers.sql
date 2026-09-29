with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_customers") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as customer_id,
            {{ adapter.quote("Fax") }} as fax,
            {{ adapter.quote("Job") }} as job,
            {{ adapter.quote("Level") }} as level,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("Mobile") }} as mobile,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("Balance") }} as balance,
            {{ adapter.quote("Taxable") }} as taxable,
            {{ adapter.quote("WebAddr") }} as web_addr,
            {{ adapter.quote("BillAddr") }} as bill_addr,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("ShipAddr") }} as ship_addr,
            {{ adapter.quote("GivenName") }} as given_name,
            {{ adapter.quote("ParentRef") }} as parent_ref,
            {{ adapter.quote("ResaleNum") }} as resale_num,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("FamilyName") }} as family_name,
            {{ adapter.quote("MiddleName") }} as middle_name,
            {{ adapter.quote("CompanyName") }} as company_name,
            {{ adapter.quote("CurrencyRef") }} as currency_ref,
            {{ adapter.quote("DisplayName") }} as display_name,
            {{ adapter.quote("PrimaryPhone") }} as primary_phone,
            {{ adapter.quote("SalesTermRef") }} as sales_term_ref,
            {{ adapter.quote("BillWithParent") }} as bill_with_parent,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("BalanceWithJobs") }} as balance_with_jobs,
            {{ adapter.quote("PaymentMethodRef") }} as payment_method_ref,
            {{ adapter.quote("PrimaryEmailAddr") }} as primary_email_addr,
            {{ adapter.quote("PrintOnCheckName") }} as print_on_check_name,
            {{ adapter.quote("DefaultTaxCodeRef") }} as default_tax_code_ref,
            {{ adapter.quote("FullyQualifiedName") }} as fully_qualified_name,
            {{ adapter.quote("PreferredDeliveryMethod") }} as preferred_delivery_method

        from source
    )
select *
from renamed
