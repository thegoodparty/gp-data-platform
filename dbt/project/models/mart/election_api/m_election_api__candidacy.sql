{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "candidacy"],
    )
}}

/*
select
    {{ generate_salted_uuid(fields=["id"], salt="ballotready") }}
    as id,
    id as br_hash_id,
    database_id as br_database_id,
    created_at,
    updated_at,
    is_certified,
    is_hidden,
    withdrawn,
    result
from {{ ref("int__ballotready_candidacy") }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}

*/
select 1 as dummy_for_eof_formatting
from (select 1)
;
