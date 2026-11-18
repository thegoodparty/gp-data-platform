-- A secondary that resolves to a third contact means the queue merged a pair whose
-- secondary had already been absorbed somewhere else. The queue's own rules should
-- stop that, so each row is a hole in them worth reading, not a routine outcome.
{{ config(severity="warn") }}

select run_id, primary_id, secondary_id, survivor_id, attempted_at
from {{ ref("stg_airflow_source__hubspot_contact_merge_log") }}
where status = 'merged_elsewhere'
