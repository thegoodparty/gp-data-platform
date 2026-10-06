-- Entity resolution crosswalk: provider raw keys -> canonical gp_* IDs.
-- One row per (provider, raw stage key); provider columns are null on rows
-- from other providers. Provider intermediates left-join this and coalesce
-- their self-mints against the canonical column: BR-anchored clusters carry
-- BR's cluster-derived ids, non-BR clusters carry the cluster's earliest-
-- member mint. Keyed on raw provider fields (not provider-computed hashes)
-- to avoid a cycle with the consuming provider models. Candidate grain is
-- not here: gp_candidate_id is the person id, resolved per provider via
-- int__civics_person_canonical_ids.
--
-- Providers:
-- TechSpeed (ts_source_candidate_id + ts_stage_election_date)
-- Product DB / gp_api (gp_api_campaign_id + gp_api_stage_election_date)
-- DDHQ (ddhq_candidate_id + ddhq_race_id)
with
    ts_stage_matches as (
        select
            {{ strip_ts_stage_suffix("ts_cw.source_id") }} as ts_source_candidate_id,
            cast(ts_cw.election_date as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
            br_cs.gp_candidacy_stage_id as canonical_gp_candidacy_stage_id,
            br_cs.gp_election_stage_id as canonical_gp_election_stage_id,
            br_cs.gp_candidacy_id as canonical_gp_candidacy_id,
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
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and ts_cw.source_name = 'techspeed'
        qualify
            row_number() over (
                partition by ts_source_candidate_id, ts_stage_election_date
                order by br_updated_at desc, canonical_gp_candidacy_id
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
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
            br_cs.gp_candidacy_stage_id as canonical_gp_candidacy_stage_id,
            br_cs.gp_election_stage_id as canonical_gp_election_stage_id,
            br_cs.gp_candidacy_id as canonical_gp_candidacy_id,
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
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and gp_cw.source_name = 'gp_api'
        qualify
            row_number() over (
                partition by gp_api_campaign_id, gp_api_stage_election_date
                order by br_updated_at desc, canonical_gp_candidacy_id
            )
            = 1
    ),

    ddhq_stage_matches as (
        select
            cast(null as string) as ts_source_candidate_id,
            cast(null as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            -- DDHQ source_id is '{candidate_id}_{race_id}' (both integers cast
            -- to string in int__er_prematch_candidacy_stages), so split on '_'.
            cast(split(ddhq_cw.source_id, '_')[0] as bigint) as ddhq_candidate_id,
            cast(split(ddhq_cw.source_id, '_')[1] as bigint) as ddhq_race_id,
            br_cs.gp_candidacy_stage_id as canonical_gp_candidacy_stage_id,
            br_cs.gp_election_stage_id as canonical_gp_election_stage_id,
            br_cs.gp_candidacy_id as canonical_gp_candidacy_id,
            br_es.gp_election_id as canonical_gp_election_id,
            br_cs.updated_at as br_updated_at
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as br_cw
        inner join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as ddhq_cw using (
                cluster_id
            )
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on br_cw.br_candidacy_id = br_cs.br_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and ddhq_cw.source_name = 'ddhq'
        qualify
            row_number() over (
                partition by ddhq_candidate_id, ddhq_race_id
                order by br_updated_at desc, canonical_gp_candidacy_id
            )
            = 1
    ),

    non_br_clusters as (
        -- Cluster_ids whose members include no BR record.
        select cluster_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
        group by cluster_id
        having count_if(source_name = 'ballotready') = 0
    ),

    -- Clusters with no vendor member: only gp_api records, or a single one.
    gp_only_clusters as (
        select cluster_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
        group by cluster_id
        having count_if(source_name <> 'gp_api') = 0
    ),

    -- A campaign whose BallotReady primary at its own position was found
    -- deterministically. Its stage is the general, which matcha keeps apart
    -- from the primary, so a signup eliminated in the primary never shares a
    -- cluster with BR. Adopting the BR candidacy (one id across stages) puts
    -- the primary result on the campaign's candidacy. Only used where no vendor
    -- clusters with the campaign: a vendor listing for the general wins.
    gp_api_br_primary as (
        select
            pr.campaign_id as primary_campaign_id,
            pr.election_date as primary_campaign_election_date,
            br_cs.gp_candidacy_id as primary_gp_candidacy_id,
            br_es.gp_election_id as primary_gp_election_id
        from {{ ref("int__civics_campaign_br_primary_result") }} as pr
        inner join
            {{ ref("int__civics_candidacy_stage_ballotready") }} as br_cs
            on pr.br_candidacy_id = br_cs.br_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        qualify
            row_number() over (
                partition by pr.campaign_id order by br_cs.gp_candidacy_id
            )
            = 1
    ),

    non_br_cluster_matches as (
        -- Non-BR clusters (no BR member to anchor to): the candidacy canonical
        -- is the cluster's earliest-member mint, shared by every co-member, so
        -- matched TS/DDHQ/gp_api rows still collapse to one mart row. Stage,
        -- election_stage, and election canonicals stay NULL: candidacy_stage
        -- merges by cluster FOJ, and race/election grains fall back to
        -- provider self-mints (cross-cluster merging there still relies on
        -- BR-anchored adoption — same topology as before).
        --
        -- Dedup on each provider's natural key (matches the BR-anchored
        -- branches' partition keys). Two TS records sharing the same stripped
        -- (source_candidate_id, election_date) but in different non-BR
        -- clusters keep one canonical, deterministically by min(cluster_id).
        select
            {{ strip_ts_stage_suffix("cw.source_id") }} as ts_source_candidate_id,
            cast(cw.election_date as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
            cast(null as string) as canonical_gp_candidacy_stage_id,
            cast(null as string) as canonical_gp_election_stage_id,
            mint.minted_gp_candidacy_id as canonical_gp_candidacy_id,
            cast(null as string) as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
        inner join
            {{ ref("int__civics_minted_candidacy_ids") }} as mint using (unique_id)
        where
            cw.source_name = 'techspeed'
            -- Skip (ts_source_candidate_id, election_date)
            -- combos already produced by ts_stage_matches above. This guards
            -- against ts_key_unique_in_crosswalk failures when a single
            -- TS person has two distinct candidacies on the same election
            -- date but in different clusters (one BR-paired, one TS-only).
            -- Root cause is upstream: candidate_code is keyed on
            -- (first_name, last_name, state, city, office_type) and
            -- office_type='other' can't distinguish specialty districts in
            -- the same city (e.g. one candidate's water-district general and
            -- sewer-district primary on the same date both collapse to the
            -- same stripped ts_source_candidate_id).
            -- The BR-paired ts_stage_matches branch owns the canonical
            -- mapping; the TS-only candidacy still gets a row in
            -- mart_civics.candidacy via int__civics_candidacy_techspeed
            -- with a TS-derived gp_candidate_id fallback. A proper fix
            -- would add official_office_name to candidate_code upstream
            -- (would re-rotate codes for many TS rows — out of scope here).
            and not exists (
                select 1
                from ts_stage_matches s
                where
                    s.ts_source_candidate_id
                    = {{ strip_ts_stage_suffix("cw.source_id") }}
                    and s.ts_stage_election_date <=> cast(cw.election_date as date)
            )
        qualify
            row_number() over (
                partition by ts_source_candidate_id, ts_stage_election_date
                order by cw.cluster_id
            )
            = 1

        union all

        select
            cast(null as string) as ts_source_candidate_id,
            cast(null as date) as ts_stage_election_date,
            cast(split(cw.source_id, '__')[0] as bigint) as gp_api_campaign_id,
            cast(cw.election_date as date) as gp_api_stage_election_date,
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
            cast(null as string) as canonical_gp_candidacy_stage_id,
            cast(null as string) as canonical_gp_election_stage_id,
            case
                when gp_only.cluster_id is not null
                then coalesce(prim.primary_gp_candidacy_id, mint.minted_gp_candidacy_id)
                else mint.minted_gp_candidacy_id
            end as canonical_gp_candidacy_id,
            case
                when gp_only.cluster_id is not null then prim.primary_gp_election_id
            end as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
        inner join
            {{ ref("int__civics_minted_candidacy_ids") }} as mint using (unique_id)
        left join gp_only_clusters as gp_only using (cluster_id)
        left join
            gp_api_br_primary as prim
            on cast(split(cw.source_id, '__')[0] as bigint) = prim.primary_campaign_id
            and cast(cw.election_date as date) = prim.primary_campaign_election_date
        where cw.source_name = 'gp_api'
        qualify
            row_number() over (
                partition by gp_api_campaign_id, gp_api_stage_election_date
                order by cw.cluster_id
            )
            = 1

        union all

        select
            cast(null as string) as ts_source_candidate_id,
            cast(null as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            cast(split(cw.source_id, '_')[0] as bigint) as ddhq_candidate_id,
            cast(split(cw.source_id, '_')[1] as bigint) as ddhq_race_id,
            cast(null as string) as canonical_gp_candidacy_stage_id,
            cast(null as string) as canonical_gp_election_stage_id,
            mint.minted_gp_candidacy_id as canonical_gp_candidacy_id,
            cast(null as string) as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
        inner join
            {{ ref("int__civics_minted_candidacy_ids") }} as mint using (unique_id)
        where cw.source_name = 'ddhq'
        qualify
            row_number() over (
                partition by ddhq_candidate_id, ddhq_race_id order by cw.cluster_id
            )
            = 1
    )

select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    ddhq_candidate_id,
    ddhq_race_id,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_election_id
from ts_stage_matches
union all
select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    ddhq_candidate_id,
    ddhq_race_id,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_election_id
from gp_api_stage_matches
union all
select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    ddhq_candidate_id,
    ddhq_race_id,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_election_id
from ddhq_stage_matches
union all
select
    ts_source_candidate_id,
    ts_stage_election_date,
    gp_api_campaign_id,
    gp_api_stage_election_date,
    ddhq_candidate_id,
    ddhq_race_id,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_election_id
from non_br_cluster_matches
union all
-- Campaigns with a BR primary match that never reached matcha (no clustered
-- record), so the gp_api models can still adopt the BR candidacy.
select
    cast(null as string) as ts_source_candidate_id,
    cast(null as date) as ts_stage_election_date,
    prim.primary_campaign_id as gp_api_campaign_id,
    prim.primary_campaign_election_date as gp_api_stage_election_date,
    cast(null as bigint) as ddhq_candidate_id,
    cast(null as bigint) as ddhq_race_id,
    cast(null as string) as canonical_gp_candidacy_stage_id,
    cast(null as string) as canonical_gp_election_stage_id,
    prim.primary_gp_candidacy_id as canonical_gp_candidacy_id,
    prim.primary_gp_election_id as canonical_gp_election_id
from gp_api_br_primary as prim
left anti join
    {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
    on cw.source_name = 'gp_api'
    and cast(split(cw.source_id, '__')[0] as bigint) = prim.primary_campaign_id
