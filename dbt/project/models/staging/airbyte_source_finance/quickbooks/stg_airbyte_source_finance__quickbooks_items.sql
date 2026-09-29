with
    source as (
        select * from {{ source("airbyte_source_finance", "quickbooks_items") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("Id") }} as item_id,
            {{ adapter.quote("Name") }} as name,
            {{ adapter.quote("Type") }} as type,
            {{ adapter.quote("Active") }} as active,
            {{ adapter.quote("domain") }},
            {{ adapter.quote("sparse") }},
            {{ adapter.quote("Taxable") }} as taxable,
            {{ adapter.quote("MetaData") }} as meta_data,
            {{ adapter.quote("QtyOnHand") }} as qty_on_hand,
            {{ adapter.quote("SyncToken") }} as sync_token,
            {{ adapter.quote("UnitPrice") }} as unit_price,
            {{ adapter.quote("Description") }} as description,
            {{ adapter.quote("InvStartDate") }} as inv_start_date,
            {{ adapter.quote("PurchaseCost") }} as purchase_cost,
            {{ adapter.quote("PurchaseDesc") }} as purchase_desc,
            {{ adapter.quote("TrackQtyOnHand") }} as track_qty_on_hand,
            {{ adapter.quote("airbyte_cursor") }},
            {{ adapter.quote("AssetAccountRef") }} as asset_account_ref,
            {{ adapter.quote("IncomeAccountRef") }} as income_account_ref,
            {{ adapter.quote("ExpenseAccountRef") }} as expense_account_ref,
            {{ adapter.quote("FullyQualifiedName") }} as fully_qualified_name

        from source
    )
select *
from renamed
