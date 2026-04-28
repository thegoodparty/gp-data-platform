-- Entity resolution crosswalk: provider raw keys -> BR canonical gp_* IDs.
-- One row per (provider, raw stage key); provider columns are null on rows
-- from other providers. Provider intermediates left-join this and coalesce
-- their own hashes against the canonical column, so clustered rows share
-- BR's IDs. Keyed on raw provider fields (not provider-computed hashes) to
-- avoid a cycle with the consuming provider models.
--
-- Providers: TechSpeed (ts_source_candidate_id + ts_stage_election_date) and
-- Product DB / gp_api (gp_api_campaign_id + gp_api_stage_election_date).
with
    ts_stage_matches as (
        select
            regexp_replace(
                ts_cw.source_id, '__(primary|general|runoff)$', ''
            ) as ts_source_candidate_id,
            cast(ts_cw.election_date as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            br_cs.gp_candidacy_stage_id as canonical_gp_candidacy_stage_id,
            br_cs.gp_election_stage_id as canonical_gp_election_stage_id,
            br_cs.gp_candidacy_id as canonical_gp_candidacy_id,
            br_c.gp_candidate_id as canonical_gp_candidate_id,
            br_es.gp_election_id as canonical_gp_election_id,
            br_cs.updated_at as br_updated_at
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as br_cw
        inner join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as ts_cw using (
                cluster_id
            )
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on br_cw.br_candidacy_id = br_cs.br_candidacy_id
        inner join
            {{ ref("int__civics_candidacy_ballotready") }} as br_c
            on br_cs.gp_candidacy_id = br_c.gp_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and ts_cw.source_name = 'techspeed'
        qualify
            row_number() over (
                partition by ts_source_candidate_id, ts_stage_election_date
                order by br_updated_at desc
            )
            = 1
    ),

    gp_api_stage_matches as (
        select
            cast(null as string) as ts_source_candidate_id,
            cast(null as date) as ts_stage_election_date,
            -- source_id is '{campaign_id}__{stage}'; stage can be a compound
            -- variant (general_runoff, primary_special_runoff, …), so split.
            cast(split(gp_cw.source_id, '__')[0] as bigint) as gp_api_campaign_id,
            cast(gp_cw.election_date as date) as gp_api_stage_election_date,
            br_cs.gp_candidacy_stage_id as canonical_gp_candidacy_stage_id,
            br_cs.gp_election_stage_id as canonical_gp_election_stage_id,
            br_cs.gp_candidacy_id as canonical_gp_candidacy_id,
            br_c.gp_candidate_id as canonical_gp_candidate_id,
            br_es.gp_election_id as canonical_gp_election_id,
            br_cs.updated_at as br_updated_at
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as br_cw
        inner join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as gp_cw using (
                cluster_id
            )
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on br_cw.br_candidacy_id = br_cs.br_candidacy_id
        inner join
            {{ ref("int__civics_candidacy_ballotready") }} as br_c
            on br_cs.gp_candidacy_id = br_c.gp_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and gp_cw.source_name = 'gp_api'
        qualify
            row_number() over (
                partition by gp_api_campaign_id, gp_api_stage_election_date
                order by br_updated_at desc
            )
            = 1
    )

select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_candidate_id,
    canonical_gp_election_id
from ts_stage_matches
union all
select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_candidate_id,
    canonical_gp_election_id
from gp_api_stage_matches
