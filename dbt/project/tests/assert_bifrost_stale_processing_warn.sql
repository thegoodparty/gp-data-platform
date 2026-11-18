{{ config(severity="warn") }}

-- A request still `processing` long after it was made will never reach a
-- terminal state in a later export, so it never reaches the mart and its cost
-- is lost silently: the mart filters `processing` out by design, which makes
-- this the only signal those rows produce. `processing` rows are otherwise
-- expected, because an export landing mid-flight observes them.
--
-- Warn, not error: a rebuild must not stop over one stuck request.
--
-- Compared against wall-clock time rather than the data's own maximum, so it
-- still fires during a traffic lull, and it selects ids rather than a count so
-- a clean run returns no rows at all. Seven days is the export's own
-- re-emission window: older rows are not exported again and cannot change.
select request_id, request_at, exported_at
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where status = 'processing' and request_at < current_timestamp() - interval 7 days
