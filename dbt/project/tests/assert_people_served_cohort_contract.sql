-- People Served cohort contract (epic DATA-1359). Guards two invariants that the
-- ordering test and the warn-severity sanity bands cannot:
-- (1) PRESENCE: each cohort ('all', 'active') must publish all four metric variants
-- at office_type='all' (the North Star headline). A broken active join would
-- silently emit zero active rows -- the ordering test only checks
-- (cohort, office_type) groups that already exist, so a wholesale-missing active
-- cohort slips through, and the sanity band is warn-only.
-- (2) NO ACTIVE STATEWIDE: cohort='active' must carry no office_type='State' rows
-- (no statewide office is Serve-ICP). Enforces the documented contract against a
-- future data drift.
-- Returns offending rows; empty = pass.
with
    headline as (
        select cohort, count(distinct metric_variant) as n_variants
        from {{ ref("people_served") }}
        where office_type = 'all'
        group by cohort
    ),

    expected_cohorts as (
        select 'all' as cohort
        union all
        select 'active' as cohort
    ),

    presence_failures as (
        select
            e.cohort,
            coalesce(h.n_variants, 0) as n_variants,
            'headline_missing_variants' as failure
        from expected_cohorts e
        left join headline h on e.cohort = h.cohort
        where coalesce(h.n_variants, 0) != 4
    ),

    active_state_failures as (
        select cohort, count(*) as n_variants, 'active_has_statewide' as failure
        from {{ ref("people_served") }}
        where cohort = 'active' and office_type = 'State'
        group by cohort
    )

select cohort, n_variants, failure
from presence_failures
union all
select cohort, n_variants, failure
from active_state_failures
