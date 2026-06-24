{{ config(materialized="table", tags=["marts", "analytics", "serve"]) }}

-- people_served (DATA-1993, epic DATA-1359): the People Served metrics, one row per
-- (cohort, metric_variant, office_type). count-once is the distinct-person union from
-- int__serve_block_coverage; count-multiple variants are pure sums off the substrate
-- via district_census_stats. Scoped to the substrate's office-bearing district types,
-- so
-- count-once <= count_multiple_per_district <= per_seat <= per_org by construction
-- (a distinct union is <= a sum; a per-district sum <= per-seat <= per-org because
-- 1 <= n_seats <= n_orgs per district).
--
-- cohort: 'all' (every resolved serve org -- the broad reading) or 'active' (the active
-- People Served cohort: in_people_served_cohort = active serve user AND Serve-ICP
-- office
-- AND not internal). 'active' is the canonical North Star; 'all' is retained for the
-- broad comparison. The single resolver flag in_people_served_cohort drives BOTH paths:
-- count-once reads served_voters_active from block-coverage, count-multiple adds the
-- flag to the district_level filter, so all four variants share one active universe.
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
-- query -- TDD 4.2). No statewide office is Serve-ICP, so cohort='active' has no
-- 'State'
-- rows.
--
-- The live 'all' numbers include population from a few known wrong-scope matches
-- (requires_review, pending the DATA-1989 upstream overrides); the conservative reading
-- excludes them. Those rows are carried + flagged on official_constituents for the
-- product
-- to caveat; they are NOT filtered here (fix upstream, not in the model -- TDD 7.3).
-- Tightening to cohort='active' drops most wrong-scope matches on its own. seat =
-- a distinct namespaced position/office id with an org_slug fallback, guaranteeing
-- 1 <= n_seats <= n_orgs per district. Thin by design: variants may migrate to the dbt
-- semantic layer.
with
    block_pop as (
        select block_geoid, population from {{ ref("stg_census__block_population") }}
    ),

    coverage as (select * from {{ ref("int__serve_block_coverage") }}),

    -- count-once per (cohort, served_set): 'all' reads served_voters, 'active' reads
    -- served_voters_active. The North Star headline is (cohort='active',
    -- office_type='all'); 'all' is the broad comparison reading.
    count_once_local as (
        select
            'all' as cohort,
            c.served_set as office_type,
            sum(c.served_voters * 1.0 / c.total_voters * p.population) as people_served
        from coverage c
        join block_pop p on c.block_geoid = p.block_geoid
        group by c.served_set

        union all

        select
            'active' as cohort,
            c.served_set as office_type,
            sum(
                c.served_voters_active * 1.0 / c.total_voters * p.population
            ) as people_served
        from coverage c
        join block_pop p on c.block_geoid = p.block_geoid
        group by c.served_set
        -- drop types with no active voters so the active office_type set matches the
        -- count-multiple active set (the ordering invariant needs all four variants per
        -- (cohort, office_type)). count_once and count-multiple gate office_type
        -- independently but share one L2-unpivot lineage, so the two sets are identical
        -- by construction; assert_people_served_cohort_contract verifies it and FAILS
        -- CLOSED on any divergence. Do NOT instead subset count_once to the cm set --
        -- that would silently drop real served population from the headline.
        having sum(c.served_voters_active) > 0
    ),

    -- one row per resolved serve org, with its seat id and cohort flag. seat id is
    -- namespaced so a BallotReady id never collides with an office id; the org_slug
    -- fallback guarantees every org contributes >=1 distinct seat.
    resolved_orgs as (
        select
            organization_slug,
            state,
            l2_district_type,
            normalized_district_name,
            is_statewide,
            in_people_served_cohort,
            coalesce(
                'br:' || cast(ballotready_position_id as string),
                'eo:' || cast(elected_office_id as string),
                'org:' || organization_slug
            ) as seat_id
        from {{ ref("int__serve_district_resolution") }}
        where resolution_path != 'unresolved'
    ),

    -- one row per cohort district (local + statewide), carrying both all-cohort and
    -- active-cohort seat/org multipliers in one pass.
    district_level as (
        select
            co.l2_district_type as office_type,
            s.district_population,
            count(distinct co.organization_slug) as n_orgs_all,
            count(distinct co.seat_id) as n_seats_all,
            count(
                distinct case
                    when co.in_people_served_cohort then co.organization_slug
                end
            ) as n_orgs_active,
            count(
                distinct case when co.in_people_served_cohort then co.seat_id end
            ) as n_seats_active
        from resolved_orgs co
        join
            {{ ref("district_census_stats") }} s
            on co.state = s.state_postal_code
            and co.l2_district_type = s.district_type
            and co.normalized_district_name = s.district_name
            and s.district_type != 'State'
        where not co.is_statewide
        group by
            co.l2_district_type,
            s.state_postal_code,
            s.district_name,
            s.district_population

        union all

        select
            'State' as office_type,
            s.district_population,
            count(distinct co.organization_slug) as n_orgs_all,
            count(distinct co.seat_id) as n_seats_all,
            count(
                distinct case
                    when co.in_people_served_cohort then co.organization_slug
                end
            ) as n_orgs_active,
            count(
                distinct case when co.in_people_served_cohort then co.seat_id end
            ) as n_seats_active
        from resolved_orgs co
        join
            {{ ref("district_census_stats") }} s
            on co.state = s.state_postal_code
            and s.district_type = 'State'
        where co.is_statewide
        group by s.state_postal_code, s.district_population
    ),

    -- explode to one row per (cohort, district), picking the cohort-appropriate
    -- multipliers. 'active' keeps only districts with >=1 active org.
    district_by_cohort as (
        select
            'all' as cohort,
            office_type,
            district_population,
            n_orgs_all as n_orgs,
            n_seats_all as n_seats
        from district_level

        union all

        select
            'active' as cohort,
            office_type,
            district_population,
            n_orgs_active as n_orgs,
            n_seats_active as n_seats
        from district_level
        where n_orgs_active > 0
    ),

    -- count-multiple per (cohort, office_type), plus a local-only 'all' rollup
    cm_by_type as (
        select
            cohort,
            office_type,
            sum(district_population) as per_district,
            sum(district_population * n_seats) as per_seat,
            sum(district_population * n_orgs) as per_org,
            count(*) as n_cohort_districts
        from district_by_cohort
        group by cohort, office_type
    ),

    cm_all as (
        select
            cohort,
            'all' as office_type,
            sum(district_population) as per_district,
            sum(district_population * n_seats) as per_seat,
            sum(district_population * n_orgs) as per_org,
            count(*) as n_cohort_districts
        from district_by_cohort
        where office_type != 'State'
        group by cohort
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
        select cohort, 'State' as office_type, per_district as people_served
        from cm_by_type
        where office_type = 'State'
    ),

    final as (
        select
            'count_once' as metric_variant,
            cohort,
            office_type,
            people_served,
            cast(null as bigint) as n_cohort_districts
        from count_once_local

        union all

        select 'count_once', cohort, office_type, people_served, cast(null as bigint)
        from statewide_count_once

        union all

        select
            'count_multiple_per_district',
            cohort,
            office_type,
            per_district,
            n_cohort_districts
        from cm
        union all
        select
            'count_multiple_per_seat', cohort, office_type, per_seat, n_cohort_districts
        from cm
        union all
        select
            'count_multiple_per_org', cohort, office_type, per_org, n_cohort_districts
        from cm
    )

select cohort, metric_variant, office_type, people_served, n_cohort_districts
from final
