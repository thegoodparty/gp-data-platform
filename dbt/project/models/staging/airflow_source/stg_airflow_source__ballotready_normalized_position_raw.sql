{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_normalized_position_raw") }}

select
    requested_id,
    loaded_at,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    get_json_object(payload, '$.description') as description,
    get_json_object(payload, '$.id') as id,
    {{ br_id_ref_array("$.issues") }} as issues,
    get_json_object(payload, '$.mtfcc') as mtfcc,
    get_json_object(payload, '$.name') as name,
    -- ingestion timestamps, not source timestamps: this payload carries no
    -- createdAt/updatedAt, and the model this replaces stamped both at run time.
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from current_rows
