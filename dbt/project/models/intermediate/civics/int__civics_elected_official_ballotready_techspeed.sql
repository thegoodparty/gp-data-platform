{{ config(materialized="table", tags=["civics"]) }}

-- BR↔TS deterministic join: BallotReady elected official terms with TechSpeed
-- contact data attached via direct ID match (ts_officeholder_id = br_office_holder_id).
-- Single source of truth consumed by the mart and future ER prematch.
--
-- TS dedup: one row per ts_officeholder_id, ordered by updated_at desc with
-- stable tie-breakers. 86 IDs (0.3%) are reused across different people in TS;
-- the dedup picks one arbitrarily (tracked as a known data quality follow-up).
--
-- Grain: One row per BR elected official term (person-position-term).
-- Row count equals int__civics_elected_official_ballotready.
with
    br as (select * from {{ ref("int__civics_elected_official_ballotready") }}),

    ts as (select * from {{ ref("int__civics_elected_official_techspeed") }}),

    -- Flag ts_officeholder_ids with conflicting is_incumbent values BEFORE dedup.
    -- 26 of 86 reused IDs have different is_incumbent across TS rows.
    ts_incumbent_conflicts as (
        select ts_officeholder_id as conflict_officeholder_id
        from ts
        group by ts_officeholder_id
        having count(distinct is_incumbent) > 1
    ),

    -- One row per ts_officeholder_id with stable ordering for deterministic selection.
    ts_position_dedup as (
        select
            try_cast(ts.ts_officeholder_id as bigint) as ts_officeholder_id_bigint,
            ts.ts_officeholder_id,
            ts.gp_elected_official_id as ts_gp_elected_official_id,
            ts.ts_position_id,
            ts.is_incumbent as ts_is_incumbent,
            ts.phone as ts_phone,
            ts.email as ts_email,
            ts.updated_at as ts_updated_at,
            ic.conflict_officeholder_id is not null as ts_incumbent_conflict
        from ts
        left join
            ts_incumbent_conflicts as ic
            on ts.ts_officeholder_id = ic.conflict_officeholder_id
        qualify
            row_number() over (
                partition by ts.ts_officeholder_id
                order by
                    ts.updated_at desc nulls last,
                    ts.ts_position_id desc nulls last,
                    ts.gp_elected_official_id desc
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
