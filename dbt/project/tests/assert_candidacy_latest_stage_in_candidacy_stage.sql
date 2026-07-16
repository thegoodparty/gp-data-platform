-- Pair-null + in-context membership invariants on candidacy.latest_stage_*.
-- latest_stage_reached is the deepest stage the candidacy has REACHED, so it may
-- be non-null while latest_stage_result is null (a stage the candidacy advanced
-- to that has no result yet). The only pairing violation is a result without a
-- stage. The in-context check is strictly tighter than plain membership: it
-- requires at least one matching candidacy_stage row whose election/position
-- aligns with the candidacy's (NULL-tolerant) and whose result matches
-- (NULL-safe, so a pending stage matches its own result-less row), preventing
-- cross-race contamination via upstream gp_candidacy_id collisions. Zero rows
-- expected.
with
    paired_null_violations as (
        select gp_candidacy_id, latest_stage_reached, latest_stage_result
        from {{ ref("candidacy") }}
        where latest_stage_result is not null and latest_stage_reached is null
    ),
    in_context_membership_violations as (
        select c.gp_candidacy_id, c.latest_stage_reached, c.latest_stage_result
        from {{ ref("candidacy") }} as c
        where
            c.latest_stage_reached is not null
            and not exists (
                select 1
                from {{ ref("candidacy_stage") }} as cs
                left join
                    {{ ref("election_stage") }} as es
                    on cs.gp_election_stage_id = es.gp_election_stage_id
                where
                    cs.gp_candidacy_id = c.gp_candidacy_id
                    and cs.election_stage = c.latest_stage_reached
                    and cs.election_result <=> c.latest_stage_result
                    and (
                        es.gp_election_id is null
                        or c.gp_election_id is null
                        or es.gp_election_id = c.gp_election_id
                    )
                    and (
                        es.br_position_id is null
                        or c.br_position_database_id is null
                        or es.br_position_id = c.br_position_database_id
                    )
            )
    )

select *
from paired_null_violations
union all
select *
from in_context_membership_violations
