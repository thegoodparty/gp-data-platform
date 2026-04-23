-- Entity resolution crosswalk: provider raw keys -> BallotReady canonical gp_* IDs.
--
-- For each provider candidacy_stage that is clustered with a BR candidacy_stage,
-- this model emits BR's gp_* IDs at every level (stage, candidacy, candidate,
-- election_stage, election). Provider intermediate models left-join this on the
-- raw keys relevant to their source and coalesce over their own hashes, so
-- clustered provider rows naturally share IDs with their BR counterparts.
--
-- Grain: one row per (provider, raw stage key). The wide schema unions all
-- providers; provider columns are null on rows from other providers.
--
-- Providers covered here:
-- - TechSpeed: keyed by ts_source_candidate_id + ts_stage_election_date.
-- - Product DB (gp_api): keyed by gp_api_campaign_id + gp_api_stage_election_date.
-- source_id format is '{campaign_id}__{stage}', where stage can include
-- compound variants (general_runoff, primary_special_runoff, etc.), so we
-- take the leading numeric prefix rather than trying to strip known suffixes.
--
-- Keyed on raw provider fields (not on provider-computed hashes) to avoid a
-- cycle with the provider intermediate models that consume this crosswalk.
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

-- Each CTE keeps the most recent BR match per provider's (raw key, stage
-- date); dropping br_updated_at in the final projection.
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
