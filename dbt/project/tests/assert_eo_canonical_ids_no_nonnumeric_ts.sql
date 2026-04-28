{{ config(severity="error") }}

-- Catches non-numeric ts_officeholder_id values. Currently 0; if any appear,
-- the unmatched-TS test silently filters them out, so this is the backstop.
-- If both fail simultaneously they'd point to a TS schema regression.
select ts_officeholder_id
from {{ ref("stg_airbyte_source__techspeed_gdrive_officeholders") }}
where ts_officeholder_id is not null and try_cast(ts_officeholder_id as bigint) is null
