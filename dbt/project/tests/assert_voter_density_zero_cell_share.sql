{{ config(severity="warn") }}

-- Zero-cell district canary. A district whose every cell falls below K publishes
-- nothing and the app hides the map. That is EXPECTED at scale: roughly 46% of
-- non-State districts hold under 2k voters, and measured coverage at that size
-- leaves many of them with no publishable cell at the finer resolutions. So a
-- high share here is normal and must not fail the build.
--
-- This is deliberately a catastrophe detector, not a tuned band: a regression in
-- the district-voter join, the H3 binning, or the K predicate shows up as nearly
-- every district publishing nothing. The 0.75 ceiling is a placeholder chosen to
-- sit well above any plausible healthy value; tighten it to a real per-resolution
-- band once the first prod populate establishes a baseline.
with
    per_resolution as (
        select
            resolution,
            count(*) as districts,
            count_if(rendered_voters = 0) as zero_cell_districts
        from {{ ref("m_people_api__district_voter_density_meta") }}
        group by resolution
    )
select
    resolution,
    districts,
    zero_cell_districts,
    zero_cell_districts * 1.0 / nullif(districts, 0) as zero_cell_share
from per_resolution
where zero_cell_districts * 1.0 / nullif(districts, 0) > 0.75
