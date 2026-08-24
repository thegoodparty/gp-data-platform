-- The no-op tripwire on the opponent fallback. Among candidacies that hold a
-- fallback row AND lack the native log_n_losers inputs (both recomputed here,
-- independent of the scorer), the scorer must show it consumed the roster
-- fill for at least the floor share. A missing join, a broken coalesce, or a
-- renamed column all surface here; a plain scored-share floor would not
-- (opponent-blind waterfall tiers score these rows regardless). Share-based:
-- the coherence gate legitimately declines a small measured share, and BR
-- roster drift moves populations, so the floor sits well under the measured
-- consumption rate. Fails too when the eligible population is empty.
with
    native_counts as (
        select gp_election_id, count(*) as n_candidates_mart
        from {{ ref("candidacy") }}
        where gp_election_id is not null
        group by gp_election_id
    ),

    eligible as (
        select fallback.gp_candidacy_id
        from {{ ref("int__civics_viability_opponents_fallback") }} as fallback
        inner join
            {{ ref("candidacy") }} as candidacy
            on fallback.gp_candidacy_id = candidacy.gp_candidacy_id
        left join
            {{ ref("election") }} as election
            on candidacy.gp_election_id = election.gp_election_id
        left join
            native_counts on candidacy.gp_election_id = native_counts.gp_election_id
        where
            not (
                election.seats_available is not null
                and election.seats_available <> 0
                and (
                    coalesce(native_counts.n_candidates_mart, 0) > 1
                    or (
                        election.number_of_opponents is not null
                        and election.number_of_opponents <> ''
                    )
                )
            )
    )

select
    count(*) as eligible_rows,
    count_if(scoring.log_n_losers_source = 'roster') as consumed_rows
from eligible
left join
    {{ ref("int__civics_viability_scoring") }} as scoring
    on eligible.gp_candidacy_id = scoring.gp_candidacy_id
having
    count(*) = 0
    or count_if(scoring.log_n_losers_source = 'roster') * 1.0 / nullif(count(*), 0)
    < 0.90
