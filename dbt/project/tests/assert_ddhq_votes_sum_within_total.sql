{{
    config(
        severity="warn",
        warn_if=">100",
        error_if=">500",
    )
}}

-- For past DDHQ-covered election stages, the sum of votes_received across
-- candidacy_stage rows should be within ±5% of election_stage.total_votes_cast.
-- Real-world tolerance: write-ins, late certifications, and DDHQ partial
-- coverage produce legitimate gaps, so we warn rather than fail. A discrepancy
-- larger than 5% is a domain-expert flag (something is meaningfully off).
with
    stage_totals as (
        select
            gp_election_stage_id,
            try_cast(total_votes_cast as bigint) as total_votes_cast
        from {{ ref("election_stage") }}
        where
            array_contains(source_systems, 'ddhq')
            and election_date < current_date()
            and try_cast(total_votes_cast as bigint) is not null
            and try_cast(total_votes_cast as bigint) > 0
    ),

    candidate_sums as (
        select
            gp_election_stage_id, sum(try_cast(votes_received as bigint)) as votes_sum
        from {{ ref("candidacy_stage") }}
        where gp_election_stage_id in (select gp_election_stage_id from stage_totals)
        group by gp_election_stage_id
    )

select
    s.gp_election_stage_id,
    s.total_votes_cast,
    c.votes_sum,
    abs(s.total_votes_cast - coalesce(c.votes_sum, 0)) / s.total_votes_cast as pct_diff
from stage_totals as s
left join candidate_sums as c on s.gp_election_stage_id = c.gp_election_stage_id
where abs(s.total_votes_cast - coalesce(c.votes_sum, 0)) / s.total_votes_cast > 0.05
