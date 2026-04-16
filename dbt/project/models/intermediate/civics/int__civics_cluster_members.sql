-- Entity resolution cluster membership with pre-computed cross-source remaps.
-- One row per (cluster_id, source). Adding a new source requires:
-- 1. One new CTE for source-specific ER join
-- 2. Add to all_members union
-- 3. Extend remap WHERE clauses to include the new source
-- 4. Add remap left joins for the new source in the final select
with
    br_members as (
        select
            cw.cluster_id,
            'ballotready' as source_name,
            br_cs.gp_candidacy_stage_id,
            br_cs.gp_candidacy_id,
            br_c.gp_candidate_id,
            br_cs.gp_election_stage_id,
            br_es.gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on cast(cw.br_candidacy_id as string) = br_cs.br_candidacy_id
        inner join
            {{ ref("int__civics_candidacy_ballotready") }} as br_c
            on br_cs.gp_candidacy_id = br_c.gp_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where cw.source_name = 'ballotready'
        qualify
            row_number() over (
                partition by cw.cluster_id order by br_cs.updated_at desc
            )
            = 1
    ),

    ts_members as (
        select
            cw.cluster_id,
            'techspeed' as source_name,
            ts_cs.gp_candidacy_stage_id,
            ts_cs.gp_candidacy_id,
            ts_c.gp_candidate_id,
            ts_cs.gp_election_stage_id,
            ts_es.gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join
            {{ ref("int__civics_candidacy_stage_techspeed") }} as ts_cs
            on regexp_replace(cw.source_id, '__(primary|general|runoff)$', '')
            = ts_cs.source_candidate_id
            and cw.election_date = ts_cs.election_stage_date
        inner join
            {{ ref("int__civics_candidacy_techspeed") }} as ts_c
            on ts_cs.gp_candidacy_id = ts_c.gp_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_techspeed") }} as ts_es
            on ts_cs.gp_election_stage_id = ts_es.gp_election_stage_id
        where cw.source_name = 'techspeed'
        qualify
            row_number() over (
                partition by cw.cluster_id order by ts_cs.updated_at desc
            )
            = 1
    ),

    all_members as (
        select *
        from br_members
        union all
        select *
        from ts_members
    ),

    -- Pre-compute FK remaps so mart models can remap TS foreign keys to BR
    -- equivalents with a simple column reference instead of repeating self-joins.
    -- Handles the cross-cluster case where a candidacy is matched via one stage
    -- but a different stage has no direct cluster match.
    -- Priority: ballotready > techspeed. Extend WHERE for new sources.
    remap_candidacy as (
        select distinct ts_m.gp_candidacy_id as ts_id, br_m.gp_candidacy_id as br_id
        from all_members as br_m
        inner join all_members as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    remap_candidate as (
        select distinct ts_m.gp_candidate_id as ts_id, br_m.gp_candidate_id as br_id
        from all_members as br_m
        inner join all_members as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    remap_election_stage as (
        select distinct
            ts_m.gp_election_stage_id as ts_id, br_m.gp_election_stage_id as br_id
        from all_members as br_m
        inner join all_members as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    ),

    remap_election as (
        select distinct ts_m.gp_election_id as ts_id, br_m.gp_election_id as br_id
        from all_members as br_m
        inner join all_members as ts_m using (cluster_id)
        where br_m.source_name = 'ballotready' and ts_m.source_name = 'techspeed'
    )

-- Enrich each member with its remap IDs (NULL for highest-priority source)
select
    m.*,
    rc.br_id as remap_candidacy_id,
    rcd.br_id as remap_candidate_id,
    res.br_id as remap_election_stage_id,
    re.br_id as remap_election_id
from all_members as m
left join
    remap_candidacy as rc
    on m.gp_candidacy_id = rc.ts_id
    and m.source_name = 'techspeed'
left join
    remap_candidate as rcd
    on m.gp_candidate_id = rcd.ts_id
    and m.source_name = 'techspeed'
left join
    remap_election_stage as res
    on m.gp_election_stage_id = res.ts_id
    and m.source_name = 'techspeed'
left join
    remap_election as re on m.gp_election_id = re.ts_id and m.source_name = 'techspeed'
