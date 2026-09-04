{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_position_election_frequency_raw") }}

select
    requested_id,
    loaded_at,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    {{ br_json_array("$.frequency", "int") }} as frequency,
    get_json_object(payload, '$.id') as id,
    cast(get_json_object(payload, '$.referenceYear') as int) as reference_year,
    from_json(get_json_object(payload, '$.seats'), 'array<int>') as seats,
    cast(get_json_object(payload, '$.validFrom') as timestamp) as valid_from,
    cast(get_json_object(payload, '$.validTo') as timestamp) as valid_to
from current_rows
