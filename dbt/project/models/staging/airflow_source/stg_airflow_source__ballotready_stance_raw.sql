{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_stance_raw") }}

select
    requested_id,
    loaded_at,
    cast(requested_id as int) as candidacy_id,
    -- the payload carries neither candidacy_id nor encoded_candidacy_id per stance
    -- element, so both are injected here; named_struct fixes prod's field order.
    transform(
        from_json(
            get_json_object(payload, '$.stances'),
            'array<struct<databaseId:int,id:string,issue:struct<databaseId:int,id:string>,locale:string,referenceUrl:string,statement:string>>'
        ),
        x -> named_struct(
            'databaseId',
            x.databaseid,
            'id',
            x.id,
            'issue',
            x.issue,
            'locale',
            x.locale,
            'referenceUrl',
            x.referenceurl,
            'statement',
            x.statement,
            'candidacy_id',
            cast(requested_id as int),
            'encoded_candidacy_id',
            get_json_object(payload, '$.id')
        )
    ) as stances,
    -- ingestion timestamps, not source timestamps: the model this replaces set both to
    -- the run time and preserved created_at across incremental runs. There is no
    -- accumulated history to carry over, so both are synthesised fresh.
    current_timestamp() as created_at,
    current_timestamp() as updated_at
from current_rows
