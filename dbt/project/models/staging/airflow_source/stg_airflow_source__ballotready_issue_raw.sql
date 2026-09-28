{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_issue_raw", preserve_created_at=true) }}

select
    requested_id,
    loaded_at,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    get_json_object(payload, '$.id') as id,
    get_json_object(payload, '$.key') as key,
    get_json_object(payload, '$.name') as name,
    cast(get_json_object(payload, '$.pluginEnabled') as boolean) as plugin_enabled,
    get_json_object(payload, '$.responseType') as response_type,
    cast(get_json_object(payload, '$.rowOrder') as int) as row_order,
    -- ingestion timestamps, not source timestamps: the issue payload carries no
    -- createdAt/updatedAt. updated_at is synthesised fresh on every run; created_at
    -- is preserved across incremental runs via br_preserved_created_at.
    {{ br_preserved_created_at() }} as created_at,
    current_timestamp() as updated_at
from current_rows
