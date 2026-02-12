{{
    config(
        materialized="table",
        tags=["intermediate", "entity_resolution"],
    )
}}

-- Entity Resolution: Final resolved match table.
-- Each row links a source record to a canonical entity (candidacy_uuid).
-- A matched pair produces two rows (one per source), enabling N-source extensibility.
with
    -- Deterministic matches (highest priority)
    deterministic as (
        select ts_record_id, br_record_id, decided_by, 1 as tier_priority
        from {{ ref("int__er_deterministic_match") }}
    ),

    -- Splink matches (medium priority)
    splink as (
        select ts_record_id, br_record_id, decided_by, 2 as tier_priority
        from {{ ref("int__er_splink_match") }}
    ),

    -- AI embedding matches (lowest priority)
    ai as (
        select ts_record_id, br_record_id, decided_by, 3 as tier_priority
        from {{ ref("int__er_ai_match") }}
    ),

    -- Union all match sources
    all_matches as (
        select *
        from deterministic
        union all
        select *
        from splink
        union all
        select *
        from ai
    ),

    -- Global 1:1 enforcement: prioritize deterministic > splink > ai_embedding.
    deduped as (
        select *
        from all_matches
        qualify
            row_number() over (
                partition by ts_record_id order by tier_priority asc, br_record_id asc
            )
            = 1
            and row_number() over (
                partition by br_record_id order by tier_priority asc, ts_record_id asc
            )
            = 1
    ),

    -- Generate a canonical UUID per matched pair
    with_uuid as (
        select
            {{
                generate_salted_uuid(
                    fields=["ts_record_id", "br_record_id"], salt="er_candidacy_match"
                )
            }} as candidacy_uuid, ts_record_id, br_record_id, decided_by
        from deduped
    ),

    -- Unpivot: one row per source record
    unpivoted as (
        -- Techspeed side
        select
            candidacy_uuid,
            'techspeed' as source_system,
            ts_record_id as source_identifier,
            decided_by
        from with_uuid

        union all

        -- BallotReady side
        select
            candidacy_uuid,
            'ballotready' as source_system,
            br_record_id as source_identifier,
            decided_by
        from with_uuid
    )

select candidacy_uuid, source_system, source_identifier, decided_by
from unpivoted
