{{
    config(
        severity="error",
        warn_if=">= 1",
        error_if=">= 2",
    )
}}

-- win_number is null in practice on the election_api race mart: BallotReady
-- supplies no win number, and the only values that ever existed are HubSpot
-- archive rows for 2023-2025 elections, which rarely resolve to a
-- BallotReady race id even now that the six-year window makes their races
-- eligible. So the win_number_effective the campaign-strategy-context
-- endpoint serves is always win_number_estimate, and that estimate is null
-- whenever a race has no positive Projected_Turnout for its election year.
--
-- This test guards that single remaining source of a win number. Among upcoming
-- races whose position resolves to a district, it tracks the share whose
-- district has no positive projected turnout for the race's election year.
-- Races whose position has no district (the unmatched_br_positions branch of
-- m_election_api__position, where district_id is null) are excluded: they can
-- never match a projected-turnout row for position-matching reasons unrelated
-- to turnout ingestion, and would otherwise inflate this metric (about half the
-- gap at authoring). Position-match coverage is a separate concern.
-- 0 rows = coverage healthy
-- 1 row  = WARN: > 15% of district-matched races lack positive projected turnout
-- 2 rows = ERROR: > 25% (both indicator rows fire)
-- Baseline at authoring: ~3.9% missing. Thresholds leave headroom for normal
-- drift while catching a projected-turnout ingestion regression that would
-- silently null the win number across the endpoint.
with
    races as (
        select
            tbl_race.id,
            year(tbl_race.election_date) as election_year,
            tbl_position.district_id
        from {{ ref("m_election_api__race") }} as tbl_race
        left join
            {{ ref("m_election_api__position") }} as tbl_position
            on tbl_race.position_id = tbl_position.id
        where
            tbl_position.district_id is not null
            -- the mart retains a 2-month grace window of recently-passed races;
            -- keep this metric scoped to upcoming races as documented above
            and tbl_race.election_date >= current_date()
    ),

    projected_turnout as (
        select district_id, election_year, max(projected_turnout) as projected_turnout
        from {{ ref("m_election_api__projected_turnout") }}
        group by district_id, election_year
    ),

    stats as (
        select
            count(*) as total_races,
            sum(
                case when coalesce(pt.projected_turnout, 0) > 0 then 0 else 1 end
            ) as missing_count
        from races
        left join
            projected_turnout as pt
            on races.district_id = pt.district_id
            and races.election_year = pt.election_year
    )

select
    'warn_gt_15pct' as breach,
    total_races,
    missing_count,
    cast(missing_count * 1.0 / nullif(total_races, 0) as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / nullif(total_races, 0) > 0.15

union all

select
    'error_gt_25pct' as breach,
    total_races,
    missing_count,
    cast(missing_count * 1.0 / nullif(total_races, 0) as decimal(5, 4)) as pct_missing
from stats
where missing_count * 1.0 / nullif(total_races, 0) > 0.25
