{{ config(severity="warn") }}

-- The suppression register is curated by hand, one insert per identifier. Past five
-- distinct requests that stops being reasonable: entries get missed, normalization
-- drifts, and nobody can say who is on the list. A warning here means the process
-- needs a managed intake, not that anything is broken. Warn-only, because a sixth
-- lawful deletion request must never block a production build.
select count(distinct request_id) as request_count
from {{ ref("stg_source_dsar__suppressed_identifiers") }}
having count(distinct request_id) > 5
