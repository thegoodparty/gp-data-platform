-- Alerting guard on the serve-cohort district resolution rate (DATA-1988).
-- Fails when the resolved share drops below 95%, or when the model is empty
-- (count(*) = 0, a total upstream break). A share floor, not a fixed count,
-- so cohort growth alone never trips it; a real drop means an upstream break
-- (district mart, crosswalk vintage, or organizations mart joins). The
-- `* 1.0` keeps the ratio an explicit float per the sibling coverage tests
-- (Databricks `/` is already float division, not integer-truncating), and
-- nullif(count(*), 0) guards the empty model from an ANSI divide-by-zero.
select
    count(*) as total_orgs,
    count(case when resolution_path != 'unresolved' then 1 end) as resolved_orgs
from {{ ref("int__serve_district_resolution") }}
having
    count(*) = 0
    or count(case when resolution_path != 'unresolved' then 1 end)
    * 1.0
    / nullif(count(*), 0)
    < 0.95
