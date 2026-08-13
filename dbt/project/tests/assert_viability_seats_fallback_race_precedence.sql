-- Race-over-position precedence: a position-tier fallback row must mean NO
-- unambiguous exact-date clean race existed for its position+date. A hit here
-- means the tier precedence broke. Recomputes the clean-race predicate
-- independently of the model on purpose.
with
    position_tier_rows as (
        select
            fallback.gp_candidacy_id,
            candidacy.br_position_database_id,
            candidacy.general_election_date
        from {{ ref("int__civics_viability_seats_fallback") }} as fallback
        inner join
            {{ ref("candidacy") }} as candidacy
            on fallback.gp_candidacy_id = candidacy.gp_candidacy_id
        where fallback.seats_source = 'position'
    ),

    clean_races as (
        select
            race.position.databaseid as br_position_database_id,
            election.election_day,
            race.seats
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_election") }} as election
            on race.election.databaseid = election.database_id
        where
            race.is_disabled = false
            and race.is_recall = false
            and race.is_primary = false
            and race.is_runoff = false
            and race.is_unexpired = false
            and race.seats > 0
    )

select position_tier_rows.gp_candidacy_id
from position_tier_rows
inner join
    clean_races
    on position_tier_rows.br_position_database_id = clean_races.br_position_database_id
    and clean_races.election_day = position_tier_rows.general_election_date
group by position_tier_rows.gp_candidacy_id
having count(distinct clean_races.seats) = 1
