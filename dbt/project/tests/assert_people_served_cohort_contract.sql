-- People Served cohort contract (epic DATA-1359). Guards three invariants that the
-- ordering test and the warn-severity sanity bands cannot:
-- (1) PRESENCE: each cohort ('all', 'active') must publish all four metric variants
-- at office_type='all' (the North Star headline). A broken active join would
-- silently emit zero active rows -- the ordering test only checks
-- (cohort, office_type) groups that already exist, so a wholesale-missing active
-- cohort slips through, and the sanity band is warn-only.
-- (2) NO ACTIVE STATEWIDE: cohort='active' must carry no office_type='State' rows
-- (no statewide office is Serve-ICP). Enforces the documented contract against a
-- future data drift.
-- (3) ENGINE ALIGNMENT: within each cohort, the office_type set carried by count_once
-- (the int__serve_block_coverage L2 voter-scan engine) must equal the set carried
-- by the count-multiple variants (the district_census_stats substrate engine). The
-- two gate office_type independently (a served voter of that type vs a census
-- district match), so in principle a type could land on one side only. They share
-- one L2-unpivot lineage (block-coverage and the substrate both unpivot
-- int__l2_nationwide_uniform via get_l2_major_district_columns +
-- normalize_l2_district_name),
-- so this cannot occur on correct data; this arm FAILS CLOSED and names the
-- direction if an upstream regression ever forks that lineage, replacing the
-- ordering test's cryptic 'missing_variant'. Deliberately does NOT subset count_once
-- to the count-multiple set: that would silently drop a real served-population row
-- from the leadership headline (a delegate-review suggestion, rejected for that
-- reason -- a divergence must halt the build, not be hidden).
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
    ),

    variant_sets as (
        select
            cohort,
            office_type,
            max(
                case when metric_variant = 'count_once' then 1 else 0 end
            ) as in_count_once,
            max(
                case
                    when
                        metric_variant in (
                            'count_multiple_per_district',
                            'count_multiple_per_seat',
                            'count_multiple_per_org'
                        )
                    then 1
                    else 0
                end
            ) as in_count_multiple
        from {{ ref("people_served") }}
        group by cohort, office_type
    ),

    set_alignment_failures as (
        select
            cohort,
            count(*) as n_variants,
            case
                when in_count_once = 1 and in_count_multiple = 0
                then 'count_once_office_type_absent_from_count_multiple'
                else 'count_multiple_office_type_absent_from_count_once'
            end as failure
        from variant_sets
        where in_count_once != in_count_multiple
        group by cohort, in_count_once, in_count_multiple
    )

select cohort, n_variants, failure
from presence_failures
union all
select cohort, n_variants, failure
from active_state_failures
union all
select cohort, n_variants, failure
from set_alignment_failures
