-- Seats fallback for the viability scorer: a BallotReady-derived seat count,
-- keyed on the candidacy's br_position_database_id, for every keyed candidacy
-- that clears the trust gates. The scorer consumes it only where the election
-- link supplies no usable seats_available, so most rows here are shadowed by
-- native election seats and never read. Fail-closed by design: rows on the
-- curated archive nullout seed, rows whose office type contradicts the
-- position crosswalk, and positions whose same-date races disagree on seats
-- produce NO row rather than a guess. seats_source records the tier:
-- 'race_exact' is a clean BR race on the candidacy's general election date
-- (same-cycle data); 'position' is the position's standing seat count. The
-- scorer consumes fallback seats for multi_seat ONLY (both tiers): its
-- opponent count is per-election grain and can span positions, so fallback
-- seats must not activate log_n_losers. The tier and matched race are kept
-- for provenance and the opponent-count follow-up. matched_br_race_id is a
-- position+date min-pick and is NOT roster-verified -- never use it to
-- select a roster.
with
    keyed_candidacies as (
        select
            gp_candidacy_id,
            gp_election_id,
            br_position_database_id,
            general_election_date,
            -- Office type recomputed from the candidacy's office-name string.
            -- The mart's office_type column would be CIRCULAR here: the mart
            -- overrides it from the same position crosswalk this model gates
            -- against, so comparing them can never fire. candidate_office is
            -- name-derived and independent of the crosswalk.
            {{ map_office_type("candidate_office") }} as name_office_type
        from {{ ref("candidacy") }}
        -- No general_election_date requirement: the position tier needs only
        -- the key. Date-less rows simply cannot match a race (or a dated
        -- nullout row), mirroring the dry-run classifier.
        where br_position_database_id is not null
    ),

    nullouts as (
        select
            gp_election_id,
            br_position_database_id,
            cast(election_date as date) as election_date
        from {{ ref("seed_civics_election_2025_position_nullouts") }}
    ),

    crosswalk as (
        select br_position_database_id, office_type as crosswalk_office_type
        from {{ ref("int__civics_position_office_type") }}
    ),

    trusted as (
        select keyed_candidacies.*
        from keyed_candidacies
        -- Two keys into the same nullout seed, and either match rejects. The
        -- seed's own tested key is gp_election_id, so it rejects the curated
        -- rows directly. The position + date proxy is kept because it
        -- additionally reaches candidacies with no election link at all -- ~70%
        -- (measured 2026-08-10)
        -- of the coverage gap has no gp_election_id, which the election-id key
        -- can never touch. Both fail closed.
        left join
            nullouts
            on keyed_candidacies.br_position_database_id
            = nullouts.br_position_database_id
            and keyed_candidacies.general_election_date = nullouts.election_date
        left join
            nullouts as nullouts_by_election
            on keyed_candidacies.gp_election_id = nullouts_by_election.gp_election_id
        left join
            crosswalk
            on keyed_candidacies.br_position_database_id
            = crosswalk.br_position_database_id
        where
            nullouts.br_position_database_id is null
            and nullouts_by_election.gp_election_id is null
            and not (
                crosswalk.crosswalk_office_type is not null
                and keyed_candidacies.name_office_type is not null
                and crosswalk.crosswalk_office_type <> 'Other'
                and keyed_candidacies.name_office_type <> 'Other'
                and crosswalk.crosswalk_office_type
                <> keyed_candidacies.name_office_type
            )
    ),

    -- Clean general-cycle races only: primaries, runoffs, recalls, disabled and
    -- unexpired-term races do not carry the cycle's seat count.
    clean_races as (
        select
            race.position.databaseid as br_position_database_id,
            election.election_day,
            race.seats,
            race.database_id as br_race_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as election
            on race.election.databaseid = election.database_id
        where {{ clean_general_race_conditions("race") }}
    ),

    race_exact as (
        select
            trusted.gp_candidacy_id,
            count(distinct clean_races.seats) as n_seat_values,
            min(clean_races.seats) as fallback_seats,
            min(clean_races.br_race_id) as matched_br_race_id
        from trusted
        inner join
            clean_races
            on trusted.br_position_database_id = clean_races.br_position_database_id
            and clean_races.election_day = trusted.general_election_date
        group by trusted.gp_candidacy_id
    ),

    position_seats as (
        select database_id as br_position_database_id, seats
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
        where seats > 0
        qualify
            row_number() over (partition by database_id order by updated_at desc) = 1
    )

-- Race tier wins. Ambiguity (same-date races disagreeing on seats) is
-- fail-closed: those candidacies get NO row at all -- they must not fall
-- through to the position tier, which would silently pick a side.
select
    gp_candidacy_id,
    fallback_seats,
    'race_exact' as seats_source,
    cast(matched_br_race_id as string) as matched_br_race_id
from race_exact
where n_seat_values = 1

union all

select
    trusted.gp_candidacy_id,
    position_seats.seats as fallback_seats,
    'position' as seats_source,
    cast(null as string) as matched_br_race_id
from trusted
inner join
    position_seats
    on trusted.br_position_database_id = position_seats.br_position_database_id
left join race_exact on trusted.gp_candidacy_id = race_exact.gp_candidacy_id
where race_exact.gp_candidacy_id is null
