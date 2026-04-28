{{ config(severity="error") }}

-- Catches future TS-only IDs directly. Asserts every numeric TS ID in
-- staging has a row in the crosswalk. If this fires, a TS-only fallback
-- canonical-id strategy must be designed before the build can proceed.
select ts.ts_officeholder_id
from
    (
        select distinct ts_officeholder_id
        from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
        where
            ts_officeholder_id is not null
            and try_cast(ts_officeholder_id as bigint) is not null
    ) ts
left join
    {{ ref("int__civics_elected_official_canonical_ids") }} xw
    on ts.ts_officeholder_id = xw.ts_officeholder_id
where xw.ts_officeholder_id is null
