-- Silent no-op guard on the seats fallback. Only fallback rows whose election
-- link supplies no usable seats are ever consumed; those rows are the whole
-- point of the model, and they must come out of int__civics_viability_scoring
-- with a score. Deliberately spans the fallback and the scorer, so it runs
-- whenever either model's tests run.
--
-- Fails when the scored share drops below the floor, or when the eligible
-- population is empty (count(*) = 0, a total upstream break). A share floor,
-- not a fixed count, so fallback volume changes never trip it on their own.
-- Measured 100.0% at introduction (2026-08-11): every eligible fallback row
-- scored. A material drop means the fallback stopped reaching the scorer.
-- `* 1.0` keeps the ratio an explicit float per the sibling coverage tests, and
-- nullif(count(*), 0) guards the empty population from an ANSI divide-by-zero.
with
    consumed_fallback as (
        select fallback.gp_candidacy_id
        from {{ ref("int__civics_viability_seats_fallback") }} as fallback
        inner join
            {{ ref("candidacy") }} as candidacy
            on fallback.gp_candidacy_id = candidacy.gp_candidacy_id
        left join
            {{ ref("election") }} as election
            on candidacy.gp_election_id = election.gp_election_id
        -- Usable native seats = not null and positive; anything else (including
        -- no election row at all) leaves the scorer on the fallback.
        where election.seats_available is null or election.seats_available <= 0
    )

select
    count(*) as eligible_fallback_rows,
    count(scoring.viability_rating_2_0) as scored_rows
from consumed_fallback
left join
    {{ ref("int__civics_viability_scoring") }} as scoring
    on consumed_fallback.gp_candidacy_id = scoring.gp_candidacy_id
having
    count(*) = 0
    or count(scoring.viability_rating_2_0) * 1.0 / nullif(count(*), 0) < 0.95
