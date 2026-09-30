select * except (_airbyte_raw_id, _airbyte_meta, _airbyte_generation_id)
from {{ ref("stg_airbyte_source__stripe_api_invoice_line_items") }}
