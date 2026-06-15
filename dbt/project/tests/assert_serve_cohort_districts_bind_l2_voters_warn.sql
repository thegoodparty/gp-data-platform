{{ config(severity="warn") }}

-- Name-drift guard for the serve cohort (DATA-1988). Every resolved,
-- non-statewide cohort district must match at least one row in
-- int__l2_district_aggregations on the normalized
-- (state, district_type, district_name) key - normalized on BOTH sides,
-- because L2 names drift between snapshots. A district that fails here
-- would silently report zero constituents downstream, so the failing rows
-- ARE the re-match queue: each one needs a corrected override or a
-- crosswalk re-match. Severity is warn (not error) while the known drift
-- backlog is worked off; flip to error once the queue is empty.
--
-- Caveat: int__l2_district_aggregations is incremental and never deletes,
-- so a district renamed in the newest L2 snapshot can still bind to a
-- stale row here. The post-refresh newly-unbound check (review-ops ticket
-- of the epic) covers that window.
with
    cohort_districts as (
        select state, l2_district_type, normalized_district_name, count(*) as n_orgs
        from {{ ref("int__serve_cohort_district") }}
        where resolution_path != 'unresolved' and not is_statewide
        group by state, l2_district_type, normalized_district_name
    ),

    l2_districts as (
        select distinct
            state_postal_code as state,
            district_type as l2_district_type,
            {{ normalize_l2_district_name("district_name") }}
            as normalized_district_name
        from {{ ref("int__l2_district_aggregations") }}
    )

select
    cohort_districts.state,
    cohort_districts.l2_district_type,
    cohort_districts.normalized_district_name,
    cohort_districts.n_orgs
from cohort_districts
left join
    l2_districts
    on cohort_districts.state = l2_districts.state
    and cohort_districts.l2_district_type = l2_districts.l2_district_type
    and cohort_districts.normalized_district_name
    = l2_districts.normalized_district_name
where l2_districts.state is null
