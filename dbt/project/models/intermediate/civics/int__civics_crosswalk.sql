{{ config(materialized="table", tags=["civics"]) }}

-- Entity resolution crosswalk pairs at all grain levels.
-- Computed once here and referenced by all civics mart models.
with
    crosswalk_br as (
        select
            cw.cluster_id,
            br_cs.gp_candidacy_stage_id,
            br_cs.gp_candidacy_id,
            br_cs.gp_election_stage_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on cast(cw.br_candidacy_id as string) = br_cs.br_candidacy_id
        where cw.source_name = 'ballotready'
        qualify
            row_number() over (
                partition by cw.cluster_id order by br_cs.updated_at desc
            )
            = 1
    ),

    crosswalk_ts as (
        select
            cw.cluster_id,
            ts_cs.gp_candidacy_stage_id,
            ts_cs.gp_candidacy_id,
            ts_cs.gp_election_stage_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join
            {{ ref("int__civics_candidacy_stage_techspeed") }} as ts_cs
            on regexp_replace(cw.source_id, '__(primary|general|runoff)$', '')
            = ts_cs.source_candidate_id
            and cw.election_date = ts_cs.election_stage_date
        where cw.source_name = 'techspeed'
        qualify
            row_number() over (
                partition by cw.cluster_id order by ts_cs.updated_at desc
            )
            = 1
    )

select distinct
    crosswalk_br.gp_candidacy_stage_id as br_candidacy_stage_id,
    crosswalk_ts.gp_candidacy_stage_id as ts_candidacy_stage_id,
    crosswalk_br.gp_candidacy_id as br_candidacy_id,
    crosswalk_ts.gp_candidacy_id as ts_candidacy_id,
    crosswalk_br.gp_election_stage_id as br_election_stage_id,
    crosswalk_ts.gp_election_stage_id as ts_election_stage_id,
    br_c.gp_candidate_id as br_candidate_id,
    ts_c.gp_candidate_id as ts_candidate_id,
    br_es.gp_election_id as br_election_id,
    ts_es.gp_election_id as ts_election_id
from crosswalk_br
inner join crosswalk_ts using (cluster_id)
inner join
    {{ ref("int__civics_candidacy_ballotready") }} as br_c
    on crosswalk_br.gp_candidacy_id = br_c.gp_candidacy_id
inner join
    {{ ref("int__civics_candidacy_techspeed") }} as ts_c
    on crosswalk_ts.gp_candidacy_id = ts_c.gp_candidacy_id
inner join
    {{ ref("int__civics_election_stage_ballotready") }} as br_es
    on crosswalk_br.gp_election_stage_id = br_es.gp_election_stage_id
inner join
    {{ ref("int__civics_election_stage_techspeed") }} as ts_es
    on crosswalk_ts.gp_election_stage_id = ts_es.gp_election_stage_id
