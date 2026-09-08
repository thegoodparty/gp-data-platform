-- A race BallotReady carries must publish BallotReady's seat count. The civics
-- stage mart holds one row per source system, and a vendor row landed on the
-- same br_race_id can state a different figure, so taking the larger of the two
-- shipped a stale seat count on live races.
select races.br_database_id, races.number_of_seats, br_stage.br_seats
from {{ ref("m_election_api__race") }} as races
join
    (
        select br_race_id, max(number_of_seats) as br_seats
        from {{ ref("election_stage") }}
        where br_race_id is not null and array_contains(source_systems, 'ballotready')
        group by br_race_id
    ) as br_stage
    on cast(races.br_database_id as string) = br_stage.br_race_id
where not (races.number_of_seats <=> br_stage.br_seats)
