-- Mint-stability canary: a ballotready member of a mixed-source cluster with
-- null first_seen_at ranks last, so a TS/DDHQ co-member would silently take
-- the mint (mass id churn if BR staging is empty or stale during a full
-- refresh). Zero rows today; any appearance means the BR first_seen lookup
-- degraded and the mint's stability guarantee is at risk.
with
    candidacy_members as (
        select
            source_name,
            first_seen_at,
            count_if(source_name != 'ballotready') over (
                partition by cluster_id
            ) as non_br_members
        from {{ ref("int__civics_minted_candidacy_ids") }}
    ),

    election_stage_members as (
        select
            source_name,
            first_seen_at,
            count_if(source_name != 'ballotready') over (
                partition by cluster_id
            ) as non_br_members
        from {{ ref("int__civics_minted_election_stage_ids") }}
    )

select 'candidacy' as grain, count(*) as br_null_in_mixed
from candidacy_members
where source_name = 'ballotready' and first_seen_at is null and non_br_members > 0
having count(*) > 0

union all

select 'election_stage', count(*)
from election_stage_members
where source_name = 'ballotready' and first_seen_at is null and non_br_members > 0
having count(*) > 0
