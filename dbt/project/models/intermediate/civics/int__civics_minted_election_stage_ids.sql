-- Minted gp_election_stage_id per election-stage record. One row per clustered
-- election-stage unique_id. The id is minted from the cluster member earliest by
-- first_seen_at (ties: source_name, source_id) and shared by all members, so it
-- is stable under cluster churn. Records in no cluster mint from themselves; the
-- consuming election_stage models supply that self-mint fallback. Replaces the
-- BR-anchored adoption. See decision 6.
with
    members as (
        select
            cl.cluster_id, cl.unique_id, cl.source_name, cl.source_id, pm.first_seen_at
        from {{ ref("stg_er_source__clustered_election_stages") }} as cl
        left join {{ ref("int__er_prematch_election_stages") }} as pm using (unique_id)
    ),

    minting_member as (
        select
            cluster_id,
            source_name as minting_source_name,
            source_id as minting_source_id
        from members
        qualify
            row_number() over (
                partition by cluster_id
                order by first_seen_at asc nulls last, source_name asc, source_id asc
            )
            = 1
    )

select
    m.unique_id,
    m.source_name,
    m.source_id,
    m.cluster_id,
    {{
        generate_salted_uuid(
            fields=["mm.minting_source_name", "mm.minting_source_id"],
            salt="election_stage",
        )
    }} as minted_gp_election_stage_id
from members as m
inner join minting_member as mm using (cluster_id)
