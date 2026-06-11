-- Alerting guard on the serve-cohort district resolution rate (DATA-1988).
-- Fails when the share of serve orgs resolving to a district drops below
-- 95%. A share floor rather than a fixed count, so cohort growth alone
-- never trips it; a real drop means an upstream break (district mart,
-- crosswalk vintage, or organizations mart joins).
select
    count(*) as total_orgs,
    count(case when resolution_path != 'unresolved' then 1 end) as resolved_orgs
from {{ ref("int__serve_district_resolution") }}
having count(case when resolution_path != 'unresolved' then 1 end) / count(*) < 0.95
