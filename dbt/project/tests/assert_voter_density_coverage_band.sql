{{ config(severity="warn") }}

-- Coverage band canary at the coarsest published resolution. Larger hexagons
-- gather more voters per cell, so fewer cells fall below K and most districts
-- should retain nearly all their voters at the coarsest resolution. A stratified
-- 36-district sample across 6 district types measured a median coverage of about
-- 0.99 there, with the single worst district at 0.61.
--
-- Warn if the median drops below 0.90. A median that low means the typical
-- district lost a tenth of its voters to suppression even at the most forgiving
-- resolution, which points at K, the binning, or the district-voter join rather
-- than at genuinely sparse geography. Checked at the median, not the minimum,
-- precisely so that sparse outliers do not trip it.
--
-- Reads the coarsest resolution present rather than a hardcoded one, so it keeps
-- working when the resolution list changes.
with
    coarsest as (
        select min(resolution) as resolution
        from {{ ref("m_people_api__district_voter_density_meta") }}
    ),
    band as (
        select
            meta.resolution,
            count(*) as districts,
            percentile_approx(meta.coverage, 0.5) as median_coverage
        from {{ ref("m_people_api__district_voter_density_meta") }} as meta
        inner join coarsest on coarsest.resolution = meta.resolution
        group by meta.resolution
    )
select resolution, districts, median_coverage
from band
where median_coverage < 0.90
