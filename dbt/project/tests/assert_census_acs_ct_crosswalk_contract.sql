{{ config(severity="error") }}

-- The Connecticut crosswalk is load-bearing: every CT decennial block remaps
-- through it downstream. The loader manifest pins the file's IDENTITY
-- (sha256); this test pins its CONTENT against two independent references:
-- the decennial block staging (bidirectional key equality -- equal counts
-- alone could hide different block sets) and the published code structure of
-- both geographies (legacy county codes on the 2020 side, planning-region
-- codes on the 2022 side, 15-digit keys throughout). The 2022 target side
-- must land entirely inside the staged ACS block-group universe, and the
-- distinct-target count must match the published planning-region block-group
-- census: 2,717, or 2,716 for a pure relabel, which cannot reach the one
-- recoded-only group.
with
    crosswalk as (
        -- RAW values, deliberately unnormalized: a malformed key (e.g. a
        -- 14-digit value missing its leading zero) must FAIL the format wall
        -- below, never be repaired into validity by padding.
        select block_fips_2020, block_fips_2022
        from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
    ),

    decennial_ct as (
        select block_geoid
        from {{ ref("stg_census__block_population") }}
        where state_fips = '09'
    ),

    staged_ct_block_groups as (
        select geoid
        from {{ ref("stg_census_acs__geo_estimates") }}
        where summary_level = '150' and left(geoid, 2) = '09'
    ),

    bad_key_format as (
        select 'bad_key_format' as violation, block_fips_2020 as detail
        from crosswalk
        where
            not (
                block_fips_2020 rlike '^09(001|003|005|007|009|011|013|015)[0-9]{10}$'
                and block_fips_2022
                rlike '^09(110|120|130|140|150|160|170|180|190)[0-9]{10}$'
            )
    ),

    crosswalk_block_missing_from_decennial as (
        select
            'crosswalk_block_missing_from_decennial' as violation,
            crosswalk.block_fips_2020 as detail
        from crosswalk
        left anti join
            decennial_ct on crosswalk.block_fips_2020 = decennial_ct.block_geoid
    ),

    decennial_block_missing_from_crosswalk as (
        select
            'decennial_block_missing_from_crosswalk' as violation,
            decennial_ct.block_geoid as detail
        from decennial_ct
        left anti join crosswalk on decennial_ct.block_geoid = crosswalk.block_fips_2020
    ),

    crosswalk_targets as (
        select distinct left(block_fips_2022, 12) as block_group_geoid from crosswalk
    ),

    target_not_in_staged_acs as (
        select
            'target_block_group_not_in_staged_acs' as violation,
            crosswalk_targets.block_group_geoid as detail
        from crosswalk_targets
        left anti join
            staged_ct_block_groups
            on crosswalk_targets.block_group_geoid = staged_ct_block_groups.geoid
    ),

    target_count_unexpected as (
        select
            'distinct_target_count_not_2716_or_2717' as violation,
            cast(count(distinct block_group_geoid) as string) as detail
        from crosswalk_targets
        having count(distinct block_group_geoid) not in (2716, 2717)
    )

select *
from bad_key_format
union all
select *
from crosswalk_block_missing_from_decennial
union all
select *
from decennial_block_missing_from_crosswalk
union all
select *
from target_not_in_staged_acs
union all
select *
from target_count_unexpected
