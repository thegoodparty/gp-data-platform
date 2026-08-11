{{ config(severity="warn") }}

-- Zero-cell district canary at the coarsest published resolution. A district whose
-- every cell falls below K publishes nothing and the app hides the map.
--
-- Checked ONLY at the coarsest resolution, deliberately. At the finest resolution
-- a large share of districts legitimately publish nothing: roughly 46% of
-- non-State districts hold under 2k voters, and the finest cells are small enough
-- that most of those suppress entirely. A threshold spanning all resolutions would
-- either warn on every run from day one or have to be set so loose it detects
-- nothing. The coarsest resolution is the one where a healthy build should have
-- very few empty districts, so it is where an empty result is actually a signal.
--
-- Reads the coarsest resolution present rather than a hardcoded one, so it keeps
-- working when the resolution list changes. The 0.25 ceiling is still a
-- provisional bound to tighten once the first prod populate gives a baseline; a
-- regression in the district-voter join, the H3 binning, or the K predicate shows
-- up here as most districts publishing nothing even at the most forgiving
-- resolution.
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
