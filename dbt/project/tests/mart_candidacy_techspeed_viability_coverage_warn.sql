{{
    config(
        severity="error",
        warn_if=">= 1",
        error_if=">= 2",
    )
}}

-- B3: emit one indicator row per breached threshold.
-- 0 rows = TS viability coverage healthy
-- 1 row  = WARN: >2.5% of TS candidacies missing viability_score
-- 2 rows = ERROR: >4% (both indicator rows fire)
-- Post-Phase-2 (DATA-1523) prod observed 2.06% missing on candidacy mart;
-- ~1,182 TS candidacies are clean-orphaned upstream (int__civics_candidacy_techspeed
-- preserves candidacy-stage grain straight from staging, while
-- int__techspeed_candidates_clean dedupes and filters out null/empty required
-- fields). Those rows can never reach viability scoring regardless of parser
-- changes. Warn 2.5% absorbs the structural floor plus small ingestion drift;
-- error 4% catches material coverage regressions. Computed dynamically against
-- the current denominator so the contract holds regardless of TS volume.
with
    stats as (
        select
            count(*) as total_ts,
            sum(case when viability_score is null then 1 else 0 end) as missing_count
        from {{ ref("candidacy") }}
        where array_contains(source_systems, 'techspeed')
    )

select
    'warn_gt_2_5pct' as breach,
    total_ts,
    missing_count,
    cast(missing_count * 1.0 / total_ts as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / total_ts > 0.025

union all

select
    'error_gt_4pct' as breach,
    total_ts,
    missing_count,
    cast(missing_count * 1.0 / total_ts as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / total_ts > 0.04
