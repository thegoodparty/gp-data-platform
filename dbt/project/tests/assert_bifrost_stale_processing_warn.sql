{{ config(severity="warn") }}

-- The mart drops `processing` by design, so a request stuck past the export's
-- 7-day window loses its cost with no other signal. Ids, not a count: count(*)
-- returns a row at zero and would warn on every clean run. Warn, so one stuck
-- request never blocks a rebuild.
select request_id, request_at, exported_at
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where status = 'processing' and request_at < current_timestamp() - interval 7 days
