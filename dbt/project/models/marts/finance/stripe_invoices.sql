select * from {{ ref("stg_airbyte_source_finance__stripe_api_invoices") }}
