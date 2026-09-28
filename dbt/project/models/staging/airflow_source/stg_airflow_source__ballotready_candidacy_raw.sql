{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_candidacy_raw") }}

select
    requested_id,
    loaded_at,
    cast(
        get_json_object(payload, '$.candidate.databaseId') as int
    ) as candidate_database_id,
    cast(get_json_object(payload, '$.createdAt') as timestamp) as created_at,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    cast(
        get_json_object(payload, '$.election.databaseId') as int
    ) as election_database_id,
    {{ br_id_ref_array("$.endorsements") }} as endorsements,
    get_json_object(payload, '$.id') as id,
    cast(get_json_object(payload, '$.isCertified') as boolean) as is_certified,
    cast(get_json_object(payload, '$.isHidden') as boolean) as is_hidden,
    {{ br_id_ref_array("$.parties") }} as parties,
    cast(
        get_json_object(payload, '$.position.databaseId') as int
    ) as position_database_id,
    cast(get_json_object(payload, '$.race.databaseId') as int) as race_database_id,
    get_json_object(payload, '$.result') as result,
    {{ br_id_ref_array("$.stances") }} as stances,
    cast(get_json_object(payload, '$.updatedAt') as timestamp) as updated_at,
    cast(get_json_object(payload, '$.withdrawn') as boolean) as withdrawn,
    -- no Airbyte feed in this path; loaded_at is the equivalent "when we last
    -- obtained this record" stamp.
    loaded_at as feed_extracted_at
from current_rows
