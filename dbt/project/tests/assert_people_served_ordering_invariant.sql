-- The People Served ordering invariant (DATA-1993): within every office_type,
-- count_once <= count_multiple_per_district <= per_seat <= per_org. count-once dedups
-- people; each count-multiple variant can only re-count them (a per-district sum >= a
-- distinct union; per-seat >= per-district and per-org >= per-seat because
-- 1 <= n_seats <= n_orgs per district). Returns offending office_types; empty = pass.
-- Null-tolerant so a legitimately-absent variant does not false-fail; the unique test
-- on
-- (metric_variant, office_type) guards against duplicate rows that max() would hide.
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

select *
from p
where
    (co is not null and cmd is not null and co > cmd + 1e-6)
    or (cmd is not null and cms is not null and cmd > cms + 1e-6)
    or (cms is not null and cmo is not null and cms > cmo + 1e-6)
