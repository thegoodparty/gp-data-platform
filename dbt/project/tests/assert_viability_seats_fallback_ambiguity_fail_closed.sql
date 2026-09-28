-- Ambiguity must fail closed: a candidacy whose exact-date clean races
-- disagree on seat counts must have NO fallback row of ANY tier (it must not
-- fall through to the position tier). Recomputes the clean-race predicate
-- independently of the model on purpose.
with
    fallback_rows as (
        select
            fallback.gp_candidacy_id,
            candidacy.br_position_database_id,
            candidacy.general_election_date
        from {{ ref("int__civics_viability_seats_fallback") }} as fallback
        inner join
            {{ ref("candidacy") }} as candidacy
            on fallback.gp_candidacy_id = candidacy.gp_candidacy_id
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

select fallback_rows.gp_candidacy_id
from fallback_rows
inner join
    clean_races
    on fallback_rows.br_position_database_id = clean_races.br_position_database_id
    and clean_races.election_day = fallback_rows.general_election_date
group by fallback_rows.gp_candidacy_id
having count(distinct clean_races.seats) > 1
