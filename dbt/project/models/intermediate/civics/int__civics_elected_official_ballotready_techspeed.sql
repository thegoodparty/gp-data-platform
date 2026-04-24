{{ config(materialized="table", tags=["civics"]) }}

-- BR↔TS deterministic join: BallotReady elected official terms with TechSpeed
-- contact data attached via direct ID match (ts_officeholder_id = br_office_holder_id).
-- Single source of truth consumed by the mart and future ER prematch.
--
-- TS dedup: one row per ts_officeholder_id, ordered by updated_at desc with
-- stable tie-breakers. 86 IDs (0.3%) are reused across different people in TS;
-- the dedup picks one deterministically (but potentially from the wrong person for 86
-- reused IDs).
--
-- Grain: One row per BR elected official term (person-position-term).
-- Row count equals int__civics_elected_official_ballotready.
with
    br as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    ts as (select * from {{ ref("int__civics_elected_official_techspeed") }}),

    -- One row per ts_officeholder_id with stable ordering for deterministic selection.
    -- ts_incumbent_conflict computed via window aggregate before dedup (26 of 86 reused
    -- IDs have conflicting is_incumbent values across TS rows).
    ts_position_dedup as (
        select
            try_cast(ts_officeholder_id as bigint) as ts_officeholder_id_bigint,
            ts_officeholder_id,
            gp_elected_official_id as ts_gp_elected_official_id,
            ts_position_id,
            is_incumbent as ts_is_incumbent,
            phone as ts_phone,
            email as ts_email,
            updated_at as ts_updated_at,
            -- COUNT(DISTINCT ...) OVER (...) unsupported on Databricks;
            -- use min/max: if they differ, there's a conflict.
            coalesce(
                min(is_incumbent) over (partition by ts_officeholder_id)
                != max(is_incumbent) over (partition by ts_officeholder_id),
                false
            ) as ts_incumbent_conflict
        from ts
        qualify
            row_number() over (
                partition by ts_officeholder_id
                order by
                    updated_at desc nulls last,
                    ts_position_id desc nulls last,
                    gp_elected_official_id desc
            )
            = 1
    )

select
    -- All BR columns (the spine)
    br.*,
    -- TS columns from direct match (NULL when no match)
    td.ts_officeholder_id,
    td.ts_gp_elected_official_id,
    td.ts_position_id,
    td.ts_phone,
    td.ts_email,
    td.ts_is_incumbent,
    coalesce(td.ts_incumbent_conflict, false) as ts_incumbent_conflict,
    td.ts_updated_at,
    td.ts_officeholder_id is not null as has_direct_ts_term_match

from br
left join
    ts_position_dedup as td on br.br_office_holder_id = td.ts_officeholder_id_bigint
