-- The hard floor, separate from the warn-level test because a test's
-- warn_if/error_if thresholds count returned rows and cannot read a percentage
-- from the result set. Below 80% coverage, ingestion is broken rather than
-- drifting and every marketing column downstream is wrong, so fail the build.
with
    coverage as (
        select count(hubspot_contact_id) * 100.0 / count(*) as pct
        from {{ ref("int__user_resolved_keys") }}
    )

select pct
from coverage
where pct < 80
