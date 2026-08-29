{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_filing_period_raw") }}

select
    requested_id,
    loaded_at,
    cast(get_json_object(payload, '$.createdAt') as timestamp) as created_at,
    -- int, not bigint: the Python model this replaces emits IntegerType and downstream
    -- marts are built against that.
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    cast(get_json_object(payload, '$.endOn') as date) as end_on,
    get_json_object(payload, '$.id') as id,
    get_json_object(payload, '$.notes') as notes,
    cast(get_json_object(payload, '$.startOn') as date) as start_on,
    get_json_object(payload, '$.type') as type,
    cast(get_json_object(payload, '$.updatedAt') as timestamp) as updated_at
from current_rows
