-- A race BallotReady carries must publish BallotReady's seat count. Skips races
-- where the BR stage row states none, which the mart fills from another source.
with
    br_stage as (
        select br_race_id, max(number_of_seats) as br_seats
        from {{ ref("election_stage") }}
        where br_race_id is not null and array_contains(source_systems, 'ballotready')
        group by br_race_id
    )
select races.br_database_id, races.number_of_seats, br_stage.br_seats
from {{ ref("m_election_api__race") }} as races
join br_stage on cast(races.br_database_id as string) = br_stage.br_race_id
where
    br_stage.br_seats is not null and not (races.number_of_seats <=> br_stage.br_seats)
