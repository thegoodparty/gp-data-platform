{{ config(severity="error") }}

-- The Connecticut crosswalk is load-bearing: every CT decennial block remaps
-- through it downstream. The loader manifest pins the file's IDENTITY
-- (sha256); this single consolidated test pins its CONTENT, one labeled
-- violation class per CTE:
-- bad_key_format: RAW values (never normalized first -- a 14-digit key
-- missing its leading zero must fail, not be repaired by padding) must
-- carry legacy county codes on the 2020 side, planning-region codes on
-- the 2022 side, 15 digits throughout.
-- suffix_or_state_changed: the 2022 recode changed only the county segment,
-- so per row the state segment and the tract+block suffix must be
-- identical across the two code systems (the pure-relabel anchor); if
-- this ever fails, the file is not a pure relabel and the populated-
-- coverage reasoning must be revisited.
-- crosswalk_block_missing_from_decennial: every crosswalk block must exist
-- in our decennial frame (strict, both populated and not).
-- populated_decennial_block_missing_from_crosswalk: every decennial CT
-- block carrying population must appear in the crosswalk; the republisher
-- omits unpopulated water-only blocks, and nothing a weighting step could
-- draw mass from may be missing.
-- target_block_group_not_in_staged_acs: every 2022 target block group must
-- exist in the staged ACS universe.
-- uncovered_populated_block_group: every staged CT block group must be
-- reachable through the crosswalk unless it provably carries no ACS mass
-- (zero population AND zero households; nulls count as populated so a
-- suppressed value can never slip through; detail carries both values for
-- diagnosis).
with
    crosswalk as (
        select block_fips_2020, block_fips_2022
        from {{ source("census_acs", "census_ct_block_to_planning_region_2022") }}
    ),

    decennial_ct as (
        select block_geoid, population
        from {{ ref("stg_census__block_population") }}
        where state_fips = '09'
    ),

    staged_ct_block_groups as (
        select geoid, total_population, households
        from {{ ref("stg_census_acs__geo_estimates") }}
        where summary_level = '150' and left(geoid, 2) = '09'
    ),

    crosswalk_targets as (
        select distinct left(block_fips_2022, 12) as block_group_geoid from crosswalk
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

    suffix_or_state_changed as (
        select 'suffix_or_state_changed' as violation, block_fips_2020 as detail
        from crosswalk
        where
            substring(block_fips_2020, 1, 2) != substring(block_fips_2022, 1, 2)
            or substring(block_fips_2020, 6) != substring(block_fips_2022, 6)
    ),

    crosswalk_block_missing_from_decennial as (
        select
            'crosswalk_block_missing_from_decennial' as violation,
            crosswalk.block_fips_2020 as detail
        from crosswalk
        left anti join
            decennial_ct on crosswalk.block_fips_2020 = decennial_ct.block_geoid
    ),

    populated_decennial_block_missing_from_crosswalk as (
        select
            'populated_decennial_block_missing_from_crosswalk' as violation,
            decennial_ct.block_geoid as detail
        from decennial_ct
        left anti join crosswalk on decennial_ct.block_geoid = crosswalk.block_fips_2020
        where decennial_ct.population > 0
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

    uncovered_populated_block_group as (
        select
            'uncovered_populated_block_group' as violation,
            concat(
                geoid,
                ' population=',
                coalesce(cast(total_population as string), 'null'),
                ' households=',
                coalesce(cast(households as string), 'null')
            ) as detail
        from staged_ct_block_groups
        where
            geoid not in (
                select block_group_geoid
                from crosswalk_targets
                where block_group_geoid is not null
            )
            and not (
                coalesce(total_population, -1) = 0 and coalesce(households, -1) = 0
            )
    )

select *
from bad_key_format
union all
select *
from suffix_or_state_changed
union all
select *
from crosswalk_block_missing_from_decennial
union all
select *
from populated_decennial_block_missing_from_crosswalk
union all
select *
from target_not_in_staged_acs
union all
select *
from uncovered_populated_block_group
