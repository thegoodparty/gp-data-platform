{{ config(severity="error") }}

-- Audit invariant: every selected_ts_officeholder_id in the TS person
-- intermediate must be a non-reused ID. If this fails, the reuse-suppression
-- filter has a regression and contaminated TS data is leaking into the
-- person mart.
select p.selected_ts_officeholder_id
from {{ ref("int__civics_elected_official_techspeed_person") }} as p
inner join
    {{ ref("int__civics_elected_official_canonical_ids") }} as xw
    on p.selected_ts_officeholder_id = xw.ts_officeholder_id
where xw.ts_officeholder_id_is_reused
