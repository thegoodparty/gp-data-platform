{{
    config(
        severity="warn",
        warn_if=">100",
        error_if=">500",
    )
}}

-- For past election_stages where DDHQ contributed results, the count of
-- candidacy_stage rows with is_winner = true should equal number_of_seats.
-- Soft threshold: real-world exceptions exist (write-ins, certifications,
-- DDHQ coverage gaps), so we warn rather than fail.
with
    stages as (
        select es.gp_election_stage_id, es.number_of_seats, es.election_date
        from {{ ref("election_stage") }} as es
        where
            array_contains(es.source_systems, 'ddhq')
            and es.election_date < current_date()
            and es.number_of_seats is not null
            and es.number_of_seats > 0
    ),

    winner_counts as (
        select cs.gp_election_stage_id, count_if(cs.is_winner) as winners
        from {{ ref("candidacy_stage") }} as cs
        inner join stages as s on cs.gp_election_stage_id = s.gp_election_stage_id
        group by cs.gp_election_stage_id
    )

select s.gp_election_stage_id, s.number_of_seats, coalesce(w.winners, 0) as winners
from stages as s
left join winner_counts as w on s.gp_election_stage_id = w.gp_election_stage_id
where coalesce(w.winners, 0) != s.number_of_seats
