{{ config(severity="warn") }}

-- Zero-cell district canary at the coarsest published resolution. A district whose
-- every cell falls below K publishes nothing and the app hides the map.
--
-- Checked ONLY at the coarsest resolution, deliberately. Renderability tracks voter
-- density, not district size: a compact 59-voter town keeps 91% of its voters at
-- r8, while a spread-out 21k-voter county fails there. So it is the genuinely
-- sparse districts that suppress entirely at the finest resolution, and a threshold
-- spanning all resolutions would have to be set so loose it detects nothing. At the
-- coarsest resolution a healthy build should have very few empty districts, so an
-- empty result there points at the district-voter join, the H3 binning, or the K
-- predicate rather than at geography.
--
-- Reads the coarsest resolution present rather than a hardcoded one, so it keeps
-- working when the resolution list changes. The 0.25 ceiling is deliberately loose:
-- a random 816-district national sample put the true share under 2% at the coarsest
-- resolution. Tighten it once the first prod populate confirms that.
with
    coarsest as (
        select min(resolution) as resolution
        from {{ ref("m_people_api__district_voter_density_meta") }}
    ),
    share as (
        select
            meta.resolution,
            count(*) as districts,
            count_if(meta.rendered_voters = 0) as zero_cell_districts
        from {{ ref("m_people_api__district_voter_density_meta") }} as meta
        inner join coarsest on coarsest.resolution = meta.resolution
        group by meta.resolution
    )
select
    resolution,
    districts,
    zero_cell_districts,
    zero_cell_districts * 1.0 / nullif(districts, 0) as zero_cell_share
from share
where zero_cell_districts * 1.0 / nullif(districts, 0) > 0.25
