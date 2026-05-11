-- Pair-null + membership invariants on candidacy.latest_stage_*. Zero rows expected.
with
    paired_null_violations as (
        select gp_candidacy_id, latest_stage_reached, latest_stage_result
        from {{ ref("candidacy") }}
        where (latest_stage_reached is null) <> (latest_stage_result is null)
    ),
    membership_violations as (
        select c.gp_candidacy_id, c.latest_stage_reached, c.latest_stage_result
        from {{ ref("candidacy") }} as c
        where
            c.latest_stage_reached is not null
            and c.latest_stage_result is not null
            and not exists (
                select 1
                from {{ ref("candidacy_stage") }} as cs
                where
                    cs.gp_candidacy_id = c.gp_candidacy_id
                    and cs.election_stage = c.latest_stage_reached
                    and cs.election_result = c.latest_stage_result
            )
    )

select *
from paired_null_violations
union all
select *
from membership_violations
