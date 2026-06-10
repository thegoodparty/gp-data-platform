{{ config(materialized="table", tags=["civics"]) }}

-- One row per BallotReady position (br_position_database_id) carrying the
-- canonical office_type for that position. office_type is derived from
-- BallotReady's normalized position name through the same two-stage transform
-- the BR candidacy path uses:
-- map_office_type(generate_candidate_office_from_position(name, normalized_name))
-- so every source can inherit a single, position-stable office_type by joining
-- on br_position_database_id (DATA-1972), instead of deriving office_type from
-- its own free-text office string. Mirrors how int__icp_offices keys on the
-- position. Built from the position universe (every BR position), not from BR
-- candidacies, so positions where only a product or TechSpeed candidate ran are
-- still covered.
with
    position as (
        select database_id, name, normalized_position
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
    ),

    normalized_position as (
        select database_id, name from {{ ref("int__ballotready_normalized_position") }}
    ),

    with_candidate_office as (
        select
            position.database_id as br_position_database_id,
            {{
                generate_candidate_office_from_position(
                    "position.name", "normalized_position.name"
                )
            }} as candidate_office
        from position
        left join
            normalized_position
            on position.normalized_position.databaseid = normalized_position.database_id
    )

select br_position_database_id, {{ map_office_type("candidate_office") }} as office_type
from with_candidate_office
where br_position_database_id is not null
