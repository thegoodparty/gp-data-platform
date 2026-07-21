{{ config(severity="error") }}

-- Drop-prevention direction, deliberately: every staged Connecticut ACS block
-- group must receive at least one 2020 block through the crosswalk, otherwise
-- that block group's ACS mass silently vanishes from the downstream remap
-- (the reverse direction can read 100 percent while mass drops). Error
-- severity with an explicit allowlist: the ACS publishes one more CT block
-- group than a pure relabel of 2020 blocks can reach, so AT MOST that one
-- known group may be allowlisted here (geoid recorded with its ACS population
-- in the PR validation evidence). Any other uncovered block group -- now or in
-- any future rebuild -- fails this test loudly.
{% set allowlisted_uncovered_block_groups = [] %}

with
    staged_ct_block_groups as (
        select geoid, total_population
        from {{ ref("stg_census_acs__geo_estimates") }}
        where summary_level = '150' and left(geoid, 2) = '09'
    ),

    crosswalk_target_block_groups as (
        -- raw values: the crosswalk contract test's format wall (error
        -- severity) already guarantees 15-digit zero-padded keys
        select distinct left(block_fips_2022, 12) as block_group_geoid
        from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
    )

select geoid as uncovered_ct_block_group, total_population
from staged_ct_block_groups
where
    geoid not in (
        select block_group_geoid
        from crosswalk_target_block_groups
        where block_group_geoid is not null
    )
    {% if allowlisted_uncovered_block_groups %}
        and geoid not in (
            {% for g in allowlisted_uncovered_block_groups %}
                '{{ g }}'{{ ", " if not loop.last }}
            {% endfor %}
        )
    {% endif %}
