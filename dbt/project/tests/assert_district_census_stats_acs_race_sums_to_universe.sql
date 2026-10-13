-- The six race and ethnicity groups partition their universe in the source, and
-- allocation multiplies every column of a block group by the same share, so the
-- allocated groups must still add up to the allocated universe. A wide gap means a
-- column was allocated on a different basis. Checked on fully covered rows only, and
-- with a 1 percent tolerance, because a block group suppressing one group but not
-- the universe legitimately leaves a small gap. Returns offending rows.
select
    state_postal_code,
    district_type,
    district_name,
    acs_population_race_universe,
    acs_population_white_nh
    + acs_population_black_nh
    + acs_population_native_american_nh
    + acs_population_asian_nh
    + acs_population_other_nh
    + acs_population_hispanic as race_group_sum
from {{ ref("district_census_stats") }}
where
    acs_coverage_share >= 1
    and acs_population_race_universe > 0
    and abs(
        acs_population_white_nh
        + acs_population_black_nh
        + acs_population_native_american_nh
        + acs_population_asian_nh
        + acs_population_other_nh
        + acs_population_hispanic
        - acs_population_race_universe
    )
    > 0.01 * acs_population_race_universe
