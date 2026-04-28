{{ config(materialized="table", tags=["civics"]) }}

-- Crosswalk: deterministic ts_officeholder_id ↔ BR canonical IDs.
-- Grain: one row per distinct numeric ts_officeholder_id that matches BR.
-- This is a key-mapping model only; per-field rollups and reused-ID
-- suppression live in int__civics_elected_official_techspeed_person.
--
-- Reads TS staging directly rather than int__civics_elected_official_techspeed
-- to (1) stay decoupled from the prematch-preserving TS intermediate and
-- (2) compute reuse against the same delivery-dedup population the TS person
-- intermediate sees downstream.
with
    ts_staging as (
        select
            *,
            trim(office_name) as office_name_clean,
            try_cast(ts_officeholder_id as bigint) as ts_officeholder_id_bigint
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        where
            ts_officeholder_id is not null
            and try_cast(ts_officeholder_id as bigint) is not null
    ),

    -- Mirror the existing TS intermediate's delivery dedup so reuse stats
    -- are computed against the same row population.
    ts_delivery_dedup as (
        select *
        from ts_staging
        qualify
            row_number() over (
                partition by ts_officeholder_id, position_id, office_name_clean
                order by date_processed_date desc nulls last, _airbyte_extracted_at desc
            )
            = 1
    ),

    -- Reuse: any ID with ≥2 records after delivery dedup. Conservative —
    -- catches both genuine cross-person contamination and legitimate
    -- same-person multi-position cases. Section 5 (TS person intermediate)
    -- suppresses contact rollup for any flagged ID. ~86 IDs in production today.
    ts_id_reuse_stats as (
        select ts_officeholder_id, count(*) as deduped_record_count
        from ts_delivery_dedup
        group by ts_officeholder_id
    ),

    ts_ids as (
        select
            td.ts_officeholder_id,
            max(td.ts_officeholder_id_bigint) as ts_officeholder_id_bigint,
            (
                max(case when rs.deduped_record_count > 1 then 1 else 0 end) = 1
            ) as ts_officeholder_id_is_reused
        from ts_delivery_dedup as td
        left join ts_id_reuse_stats as rs using (ts_officeholder_id)
        group by td.ts_officeholder_id
    )

select
    ts.ts_officeholder_id,
    br.br_office_holder_id,
    br.br_candidate_id,
    br.gp_elected_official_term_id as canonical_gp_elected_official_term_id,
    br.gp_elected_official_id as canonical_gp_elected_official_id,
    ts.ts_officeholder_id_is_reused
from ts_ids as ts
inner join
    {{ ref("int__civics_elected_official_ballotready") }} as br
    on ts.ts_officeholder_id_bigint = br.br_office_holder_id
