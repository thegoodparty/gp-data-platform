{{ config(severity="warn") }}

-- Past five hand-curated requests the register needs a managed intake. Warn only: a
-- lawful deletion request must never block a build.
select count(distinct request_id) as request_count
from {{ ref("stg_source_dsar__suppressed_identifiers") }}
having count(distinct request_id) > 5
