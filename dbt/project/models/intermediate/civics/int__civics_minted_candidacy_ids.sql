-- Minted gp_candidacy_id per candidacy-stage record. One row per clustered
-- candidacy-stage unique_id. Each candidacy-stage cluster is single-date (0
-- clusters span >1 date in prod), so its earliest-member mint is stage-grain.
-- gp_candidacy_id is candidacy-grain, so the consuming provider models roll this
-- up (min over their candidacy grouping); co-clustered candidacies converge
-- because they share the same clusters. Records in no cluster mint from
-- themselves via the provider self-mint fallback. See decision 6.
with
    members as (
        select
            cl.cluster_id, cl.unique_id, cl.source_name, cl.source_id, pm.first_seen_at
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cl
        left join {{ ref("int__er_prematch_candidacy_stages") }} as pm using (unique_id)
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
    -- BR consumers only adopt the mint from sole-BR-member clusters:
    -- matcher-merged duplicate BR records keep distinct published ids
    -- (row-preserving), while vendor co-members still adopt one BR id via
    -- the crosswalk.
    count_if(m.source_name = 'ballotready') over (
        partition by m.cluster_id
    ) as cluster_br_members,
    {{
        generate_salted_uuid(
            fields=["mm.minting_source_name", "mm.minting_source_id"],
            salt="candidacy",
        )
    }} as minted_gp_candidacy_id
from members as m
inner join minting_member as mm using (cluster_id)
