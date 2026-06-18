{{ config(materialized="table", tags=["marts", "analytics", "serve"]) }}

-- people_served (DATA-1993): the People Served metrics, one row per
-- (metric_variant, office_type). count-once is the distinct-person union from
-- int__serve_block_coverage; count-multiple variants are pure sums off the substrate
-- via district_census_stats. Scoped to the substrate's office-bearing district types,
-- so
-- count-once <= count_multiple_per_district <= per_seat <= per_org by construction
-- (a distinct union is <= a sum; a per-district sum <= per-seat <= per-org because
-- 1 <= n_seats <= n_orgs per district).
--
-- office_type: 'all' (the cohort-wide local rollup -- the North Star headline) or one
-- of
-- the substrate's local types, PLUS 'State' for STATEWIDE officials. Statewide is
-- read from T5's
-- EXACT 'State' census rows and reported SEPARATELY (TDD 4.5): it is never folded
-- into the
-- local 'all' rollup (which would mix population bases and swamp the local story). The
-- deduped local+statewide union is intentionally not a stored column (set-relative; a
-- fresh
-- query -- TDD 4.2).
--
-- The live 'all' numbers include population from a few known wrong-scope matches
-- (requires_review, pending the DATA-1989 upstream overrides); the conservative reading
-- excludes them. Those rows are carried + flagged on official_constituents for the
-- product
-- to caveat; they are NOT filtered here (fix upstream, not in the model -- TDD 7.3).
-- seat =
-- a distinct namespaced position/office id with an org_slug fallback, guaranteeing
-- 1 <= n_seats <= n_orgs per district. Thin by design: variants may migrate to the dbt
-- semantic layer.
with
    block_pop as (
        select block_geoid, population from {{ ref("stg_census__block_population") }}
    ),

    coverage as (select * from {{ ref("int__serve_block_coverage") }}),

    -- count-once per served_set (local: 'all' = the North Star, plus per type)
    count_once_local as (
        select
            c.served_set as office_type,
            sum(c.served_voters * 1.0 / c.total_voters * p.population) as people_served
        from coverage c
        join block_pop p on c.block_geoid = p.block_geoid
        group by c.served_set
    ),

    -- one row per cohort district (local + statewide), with seat/org multipliers.
    -- seat id is namespaced so a BallotReady id never collides with an office id; the
    -- org_slug fallback guarantees every org contributes >=1 distinct seat.
    district_level as (
        select
            co.l2_district_type as office_type,
            s.district_population,
            count(distinct co.organization_slug) as n_orgs,
            count(
                distinct coalesce(
                    'br:' || cast(co.ballotready_position_id as string),
                    'eo:' || cast(co.elected_office_id as string),
                    'org:' || co.organization_slug
                )
            ) as n_seats
        from {{ ref("int__serve_district_resolution") }} co
        join
            {{ ref("district_census_stats") }} s
            on co.state = s.state_postal_code
            and co.l2_district_type = s.district_type
            and co.normalized_district_name = s.district_name
            and s.district_type != 'State'
        where co.resolution_path != 'unresolved' and not co.is_statewide
        group by
            co.l2_district_type,
            s.state_postal_code,
            s.district_name,
            s.district_population

        union all

        select
            'State' as office_type,
            s.district_population,
            count(distinct co.organization_slug) as n_orgs,
            count(
                distinct coalesce(
                    'br:' || cast(co.ballotready_position_id as string),
                    'eo:' || cast(co.elected_office_id as string),
                    'org:' || co.organization_slug
                )
            ) as n_seats
        from {{ ref("int__serve_district_resolution") }} co
        join
            {{ ref("district_census_stats") }} s
            on co.state = s.state_postal_code
            and s.district_type = 'State'
        where co.resolution_path != 'unresolved' and co.is_statewide
        group by s.state_postal_code, s.district_population
    ),

    -- count-multiple per office_type (the substrate types + 'State'), and a
    -- local-only 'all'
    -- rollup
    cm_by_type as (
        select
            office_type,
            sum(district_population) as per_district,
            sum(district_population * n_seats) as per_seat,
            sum(district_population * n_orgs) as per_org,
            count(*) as n_cohort_districts
        from district_level
        group by office_type
    ),

    cm_all as (
        select
            'all' as office_type,
            sum(district_population) as per_district,
            sum(district_population * n_seats) as per_seat,
            sum(district_population * n_orgs) as per_org,
            count(*) as n_cohort_districts
        from district_level
        where office_type != 'State'
    ),

    cm as (
        select *
        from cm_by_type
        union all
        select *
        from cm_all
    ),

    -- statewide count-once = the distinct-state population sum (no inter-state
    -- overlap),
    -- which equals statewide per_district -> add it so 'State' has all four variants.
    statewide_count_once as (
        select 'State' as office_type, per_district as people_served
        from cm_by_type
        where office_type = 'State'
    ),

    final as (
        select
            'count_once' as metric_variant,
            office_type,
            people_served,
            cast(null as bigint) as n_cohort_districts
        from count_once_local

        union all

        select 'count_once', office_type, people_served, cast(null as bigint)
        from statewide_count_once

        union all

        select
            'count_multiple_per_district', office_type, per_district, n_cohort_districts
        from cm
        union all
        select 'count_multiple_per_seat', office_type, per_seat, n_cohort_districts
        from cm
        union all
        select 'count_multiple_per_org', office_type, per_org, n_cohort_districts
        from cm
    )

select metric_variant, office_type, people_served, n_cohort_districts
from final
