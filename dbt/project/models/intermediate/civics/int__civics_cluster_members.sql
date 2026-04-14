-- Entity resolution cluster membership: one row per (cluster_id, source).
-- Adding a new source = one more CTE + union leg.
-- Referenced by all civics mart models for cross-source merging and FK remapping.
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
    )

select *
from br_members
union all
select *
from ts_members
