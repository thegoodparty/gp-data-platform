{{
    config(
        severity="error",
        warn_if=">= 1",
        error_if=">= 2",
    )
}}

-- B3: emit one indicator row per breached threshold.
-- 0 rows = TS viability coverage healthy
-- 1 row  = WARN: >1% of TS candidacies missing viability_score
-- 2 rows = ERROR: >2% (both indicator rows fire)
-- Empirical MLflow floor is ~0.3%; 1% absorbs legitimate skips plus
-- small ingestion drift. Computed dynamically against the current
-- denominator so the contract holds regardless of TS volume drift
-- (prior fixed-integer thresholds went stale with population shifts).
with
    stats as (
        select
            count(*) as total_ts,
            sum(case when viability_score is null then 1 else 0 end) as missing_count
        from {{ ref("candidacy") }}
        where array_contains(source_systems, 'techspeed')
    )

select
    'warn_gt_1pct' as breach,
    total_ts,
    missing_count,
    cast(missing_count * 1.0 / total_ts as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / total_ts > 0.01

union all

select
    'error_gt_2pct' as breach,
    total_ts,
    missing_count,
    cast(missing_count * 1.0 / total_ts as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / total_ts > 0.02
