{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_party_raw", preserve_created_at=true) }}

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
    -- ingestion timestamps for this row, not the payload's own createdAt/updatedAt
    -- (those live inside each parties[] element above). updated_at is synthesised
    -- fresh on every run; created_at is preserved via br_preserved_created_at.
    {{ br_preserved_created_at() }} as created_at,
    current_timestamp() as updated_at
from current_rows
