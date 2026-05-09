-- Regression guard for candidacy.latest_stage_reached / latest_stage_result.
-- Two invariants:
-- 1. Pair-null: both columns are null or both are populated.
-- 2. Membership: every populated pair traces to a real candidacy_stage row
-- with a matching stage_type and election_result.
-- Zero rows expected.
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
                join
                    {{ ref("election_stage") }} as es
                    on cs.gp_election_stage_id = es.gp_election_stage_id
                where
                    cs.gp_candidacy_id = c.gp_candidacy_id
                    and lower(es.stage_type) = lower(c.latest_stage_reached)
                    and cs.election_result = c.latest_stage_result
            )
    )

select *
from paired_null_violations
union all
select *
from membership_violations
