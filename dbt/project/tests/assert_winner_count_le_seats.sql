{{
    config(
        severity="warn",
        warn_if=">50",
        error_if=">200",
    )
}}

-- The number of winners declared in any election stage cannot exceed the
-- number of seats on the ballot. This catches duplicate joining, bad
-- crosswalk matches, or upstream data corruption that would inflate the
-- winner count. Stricter than winner_count_matches_seats — that test
-- expects equality (which has many real-world exceptions); this one only
-- catches strictly-too-many.
with
    stages as (
        select gp_election_stage_id, number_of_seats
        from {{ ref("election_stage") }}
        where number_of_seats is not null and number_of_seats > 0
    ),

    winner_counts as (
        select cs.gp_election_stage_id, count_if(cs.is_winner) as winners
        from {{ ref("candidacy_stage") }} as cs
        where cs.gp_election_stage_id in (select gp_election_stage_id from stages)
        group by cs.gp_election_stage_id
    )

select s.gp_election_stage_id, s.number_of_seats, w.winners
from stages as s
inner join winner_counts as w on s.gp_election_stage_id = w.gp_election_stage_id
where w.winners > s.number_of_seats
