-- The People Served invariant (DATA-1993): within every office_type, all four metric
-- variants are PRESENT and count_once <= count_multiple_per_district <= per_seat <=
-- per_org. count-once dedups people; each count-multiple variant can only re-count them
-- (a per-district sum >= a distinct union; per-seat >= per-district and per-org >=
-- per-seat because 1 <= n_seats <= n_orgs per district). Strict on presence too: every
-- office_type ('all', each L2 type, and 'State') carries all four variants by
-- construction, so a missing one is a build bug -- the null-tolerant form would hide
-- it.
-- Returns offending office_types; empty = pass.
with
    p as (
        select
            office_type,
            max(case when metric_variant = 'count_once' then people_served end) as co,
            max(
                case
                    when metric_variant = 'count_multiple_per_district'
                    then people_served
                end
            ) as cmd,
            max(
                case
                    when metric_variant = 'count_multiple_per_seat' then people_served
                end
            ) as cms,
            max(
                case
                    when metric_variant = 'count_multiple_per_org' then people_served
                end
            ) as cmo
        from {{ ref("people_served") }}
        group by office_type
    )

select
    *,
    case
        when co is null or cmd is null or cms is null or cmo is null
        then 'missing_variant'
        else 'ordering_violation'
    end as failure
from p
where
    co is null
    or cmd is null
    or cms is null
    or cmo is null
    or co > cmd + 1e-6
    or cmd > cms + 1e-6
    or cms > cmo + 1e-6
