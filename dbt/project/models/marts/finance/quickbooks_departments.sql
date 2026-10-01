select * except (_airbyte_raw_id, _airbyte_meta, _airbyte_generation_id, airbyte_cursor)
from {{ ref("stg_airbyte_source_finance__quickbooks_departments") }}
