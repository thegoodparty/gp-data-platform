{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="requested_id",
    )
}}

{{ br_current_rows("ballotready_measure_raw") }}

select
    requested_id,
    loaded_at,
    -- Listed whole each run rather than fetched by id, so the run that last landed
    -- a row is the only sign a measure is still on BallotReady's upcoming list.
    extracted_at,
    dag_run_id,
    cast(get_json_object(payload, '$.databaseId') as int) as database_id,
    get_json_object(payload, '$.id') as id,
    get_json_object(payload, '$.name') as name,
    get_json_object(payload, '$.title') as title,
    get_json_object(payload, '$.summary') as summary,
    get_json_object(payload, '$.text') as text,
    get_json_object(payload, '$.proSnippet') as pro_snippet,
    get_json_object(payload, '$.conSnippet') as con_snippet,
    get_json_object(payload, '$.state') as state,
    get_json_object(payload, '$.geoId') as geo_id,
    get_json_object(payload, '$.mtfcc') as mtfcc,
    cast(
        get_json_object(payload, '$.hasUnknownBoundaries') as boolean
    ) as has_unknown_boundaries,
    cast(
        get_json_object(payload, '$.election.databaseId') as int
    ) as election_database_id,
    cast(get_json_object(payload, '$.election.electionDay') as date) as election_day,
    get_json_object(payload, '$.election.name') as election_name,
    cast(get_json_object(payload, '$.issue.databaseId') as int) as issue_database_id,
    cast(get_json_object(payload, '$.party.databaseId') as int) as party_database_id,
    get_json_object(payload, '$.arguments') as arguments,
    get_json_object(payload, '$.endorsements') as endorsements,
    cast(get_json_object(payload, '$.createdAt') as timestamp) as created_at,
    cast(get_json_object(payload, '$.updatedAt') as timestamp) as updated_at
from current_rows
