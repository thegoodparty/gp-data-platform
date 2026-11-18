{{ config(severity="warn") }}

-- A request still `processing` well after it was made will never reach a terminal state in any
-- later export, so it never reaches the mart, and its cost is lost silently: the mart filters
-- `processing` out by design. This is the only signal those rows produce. `processing` rows are
-- otherwise expected, because an export that lands mid-flight observes them.
--
-- Warn, not error: a rebuild must not stop over one stuck request.
--
-- Compares against wall-clock time rather than the data's own max, so it still fires during a
-- traffic lull, and selects the ids rather than a count so a clean run returns no rows at all.
-- Seven days is the export's own re-emission window: an older row will not be exported again and
-- so cannot change.
select request_id, request_at, exported_at
from {{ ref("stg_airflow_source__bifrost_request_log") }}
where
    status = 'processing'
    and request_at < current_timestamp() - interval 7 days
