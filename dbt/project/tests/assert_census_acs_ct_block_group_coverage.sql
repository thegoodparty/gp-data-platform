{{ config(severity="error") }}

-- Drop-prevention direction: every POPULATED staged Connecticut ACS block
-- group must receive at least one 2020 block through the crosswalk, otherwise
-- its ACS mass would silently vanish from the downstream remap (the reverse
-- direction can read complete while mass drops). The mapping's republisher
-- omits unpopulated water-only geography, so the gate is restricted to
-- geographies carrying any ACS mass at all: an uncovered block group FAILS
-- unless it has zero population and zero households -- self-verifying from
-- staged values, with no pinned geoid list to go stale, and nothing a
-- weighting step could ever draw mass from is exempt. Nulls count as
-- populated so a suppressed value can never slip through the exemption.
-- Failure rows carry population and households for instant diagnosis.
with
    staged_ct_block_groups as (
        select geoid, total_population, households
        from {{ ref("stg_census_acs__geo_estimates") }}
        where summary_level = '150' and left(geoid, 2) = '09'
    ),

    crosswalk_target_block_groups as (
        -- raw values: the crosswalk contract test's format wall (error
        -- severity) already guarantees 15-digit zero-padded keys
        select distinct left(block_fips_2022, 12) as block_group_geoid
        from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
    )

select geoid as uncovered_ct_block_group, total_population, households
from staged_ct_block_groups
where
    geoid not in (
        select block_group_geoid
        from crosswalk_target_block_groups
        where block_group_geoid is not null
    )
    and not (coalesce(total_population, -1) = 0 and coalesce(households, -1) = 0)
