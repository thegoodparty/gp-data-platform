{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_party_raw") }}

select
    requested_id,
    loaded_at,
    cast(requested_id as int) as candidacy_id,
    -- the payload's party elements already carry all six prod fields in prod's
    -- order, so no injection or reordering is needed here.
    from_json(
        get_json_object(payload, '$.parties'),
        'array<struct<createdAt:timestamp,databaseId:int,id:string,name:string,shortName:string,updatedAt:timestamp>>'
    ) as parties,
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from current_rows
