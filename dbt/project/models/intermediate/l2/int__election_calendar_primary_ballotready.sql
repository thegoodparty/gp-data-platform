-- Live-recomputed Primary date per (state, year), informed by BallotReady's
-- scheduled-election data and treated as authoritative for Primary in
-- m_election_api__election_calendar: this always reflects BallotReady's
-- CURRENT data, so an edit there (a corrected date, a newly scheduled state)
-- flows through to Election_Calendar on the next sync automatically -- no
-- script to re-run. BallotReady's election table changes rarely, so
-- recomputing this fully every run is cheap; there's no need for anything
-- fancier than "always trust the current query result."
--
-- Even years only: `Primary` is specifically an even-year concept in this
-- system (it exists to serve the even_year_primary turnout model). Odd-year
-- races always resolve to LocalOrMunicipal regardless of what BallotReady
-- calls them, so an odd-year "Primary"-named row here would be unused dead
-- data at best and a confusing surprise match at worst.
--
-- Rule (validated to 55/55 against L2 ground truth for every (state, year)
-- pair L2 could confirm as of 2024 + the 6 states L2 had already confirmed
-- for 2026, then re-checked against the FULL even-year L2 history back to
-- 2010: 198/202 matches, the only 4 misses being pre-2024 BallotReady data
-- gaps/naming quirks with no bearing on current years. Rejected alternatives:
-- max-race-count with no Wisconsin exception (53/55), and picking whichever
-- candidate falls closest to the November general day (51/55)):
--
-- Filter to elections plausibly named as each state's Primary day, then per
-- (state, year) the one with the MOST races wins -- except Wisconsin, which
-- gets the OPPOSITE (fewest races), because WI statute runs two structurally
-- distinct primaries every even year on two fixed calendar windows: a
-- nonpartisan spring primary (Feb, Wis. Stat. secs. 5.02(21)/8.05, for
-- local/judicial/school-board races that drew 3+ candidates -- thousands of
-- races) and a partisan primary for state/federal partisan office (Aug, Wis.
-- Stat. sec. 8.15 -- a few hundred races). Both are literally named
-- "Wisconsin Primary Election" in BallotReady's data with nothing else
-- distinguishing them, and the spring one always structurally outsizes the
-- partisan one -- so "most races wins" is backwards specifically for
-- Wisconsin. Confirmed against 3 independently L2-verified cycles (2020,
-- 2022, 2024) before treating this as a rule rather than a coincidence.
--
-- Also excluded: a candidate whose date exactly equals that year's computed
-- November general-election day (2 U.S.C. sec. 7 -- the Tuesday next after
-- the first Monday in November). Found to be necessary for Louisiana
-- specifically: LA has run an all-candidate "jungle primary" on the standard
-- November election day itself since ~2010 (confirmed against L2, which
-- stopped having a distinct Primary_YYYY column for LA around the same time)
-- -- BallotReady still labels that event "Louisiana Primary Election," which
-- would otherwise collide with LA's General row on the exact same (state,
-- date) key. Checked across every year in BallotReady's table (2018-2030):
-- only LA ever hits this, so it's a generic date-collision guard, not an
-- LA-specific hardcode.
--
-- If the primary-day filter finds zero rows for a (state, year) -- confirmed
-- to happen for DC in 2024, where BallotReady split that cycle into party-
-- specific "Republican/Democratic Presidential Primary Election" rows with
-- no plain "Primary Election" row at all -- fall back to including
-- Presidential-named rows, same most/fewest-races logic. Generic, not DC-
-- specific: DC's other cycles (2018-2030) all have a single plain "Primary
-- Election" row and never hit this branch.

{% set wisconsin_states = "('WI')" %}

-- next_day(d, 'MON') returns the first Monday strictly after d, so
-- subtracting a day from Nov 1 first means a Nov-1-is-a-Monday year still
-- resolves to Nov 1 itself, not Nov 8.
{% set computed_general_date_expr = "date_add(next_day(make_date(year(election_day), 11, 1) - interval 1 day, 'MON'), 1)" %}

with candidates_no_presidential as (
    select
        state,
        election_day,
        race_count,
        year(election_day) as election_year
    from {{ ref('stg_airbyte_source__ballotready_api_election') }}
    where name like '%Primary%'
      and name not like '%Runoff%'
      and name not like '%Consolidated%'
      and name not like '%Special%'
      and name not like '%Presidential%'
      and is_special = false
      and race_count > 0
      and year(election_day) % 2 = 0
      and election_day != {{ computed_general_date_expr }}
),

candidates_with_presidential as (
    select
        state,
        election_day,
        race_count,
        year(election_day) as election_year
    from {{ ref('stg_airbyte_source__ballotready_api_election') }}
    where name like '%Primary%'
      and name not like '%Runoff%'
      and name not like '%Consolidated%'
      and name not like '%Special%'
      and is_special = false
      and race_count > 0
      and year(election_day) % 2 = 0
      and election_day != {{ computed_general_date_expr }}
),

ranked_no_presidential as (
    select
        *,
        row_number() over (
            partition by state, election_year
            -- Wisconsin: fewest races wins (see header). Everyone else: most
            -- races wins. Folding both into one ORDER BY direction (ASC) via
            -- the sign flip keeps this a single ranked CTE instead of a
            -- union of two differently-ordered ones.
            order by (case when state in {{ wisconsin_states }} then race_count else -race_count end) asc
        ) as rn
    from candidates_no_presidential
),

ranked_with_presidential as (
    select
        *,
        row_number() over (
            partition by state, election_year
            order by (case when state in {{ wisconsin_states }} then race_count else -race_count end) asc
        ) as rn
    from candidates_with_presidential
),

picked_no_presidential as (
    select state, election_year, election_day
    from ranked_no_presidential
    where rn = 1
),

-- Only used for (state, year) keys picked_no_presidential has nothing for
-- (the DC-2024-style fallback) -- see the final select's coalesce.
picked_with_presidential as (
    select state, election_year, election_day
    from ranked_with_presidential
    where rn = 1
)

select
    coalesce(a.state, b.state) as state,
    coalesce(a.election_day, b.election_day) as election_date,
    'Primary' as election_code
from picked_no_presidential a
full outer join picked_with_presidential b
    on a.state = b.state and a.election_year = b.election_year
