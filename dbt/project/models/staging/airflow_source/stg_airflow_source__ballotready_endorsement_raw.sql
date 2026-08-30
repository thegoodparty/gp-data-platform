{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_endorsement_raw", preserve_created_at=true) }}

select
    requested_id,
    loaded_at,
    cast(requested_id as int) as candidacy_id,
    -- the payload carries neither candidacy_id nor encoded_candidacy_id per endorsement
    -- element, so both are injected here; named_struct fixes prod's field order, which
    -- does not match the JSON's key order.
    transform(
        from_json(
            get_json_object(payload, '$.endorsements'),
            'array<struct<databaseId:int,id:string,createdAt:timestamp,endorser:string,recommendation:string,status:string,updatedAt:timestamp,organization:struct<databaseId:int,id:string>>>'
        ),
        x -> named_struct(
            'databaseId',
            x.databaseid,
            'id',
            x.id,
            'createdAt',
            x.createdat,
            'endorser',
            x.endorser,
            'recommendation',
            x.recommendation,
            'status',
            x.status,
            'updatedAt',
            x.updatedat,
            'organization',
            x.organization,
            'candidacy_id',
            cast(requested_id as int),
            'encoded_candidacy_id',
            get_json_object(payload, '$.id')
        )
    ) as endorsements,
    -- ingestion timestamps, not source timestamps. updated_at is synthesised fresh on
    -- every run; created_at is preserved across incremental runs via
    -- br_preserved_created_at.
    {{ br_preserved_created_at() }} as created_at,
    current_timestamp() as updated_at
from current_rows
