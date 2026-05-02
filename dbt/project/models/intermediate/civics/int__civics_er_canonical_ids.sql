-- Entity resolution crosswalk: provider raw keys -> BR canonical gp_* IDs.
-- One row per (provider, raw stage key); provider columns are null on rows
-- from other providers. Provider intermediates left-join this and coalesce
-- their own hashes against the canonical column, so clustered rows share
-- BR's IDs. Keyed on raw provider fields (not provider-computed hashes) to
-- avoid a cycle with the consuming provider models.
--
-- Providers:
-- TechSpeed (ts_source_candidate_id + ts_stage_election_date)
-- Product DB / gp_api (gp_api_campaign_id + gp_api_stage_election_date)
-- DDHQ (ddhq_candidate_id + ddhq_race_id)
with
    ts_stage_matches as (
        select
            regexp_replace(
                ts_cw.source_id, '__(primary|general|runoff)$', ''
            ) as ts_source_candidate_id,
            cast(ts_cw.election_date as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
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
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
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
            br_c.gp_candidate_id as canonical_gp_candidate_id,
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
            {{ ref("int__civics_candidacy_ballotready") }} as br_c
            on br_cs.gp_candidacy_id = br_c.gp_candidacy_id
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on br_cs.gp_election_stage_id = br_es.gp_election_stage_id
        where br_cw.source_name = 'ballotready' and ddhq_cw.source_name = 'ddhq'
        qualify
            row_number() over (
                partition by ddhq_candidate_id, ddhq_race_id order by br_updated_at desc
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

    non_br_cluster_matches as (
        -- For non-BR clusters, derive canonical IDs from cluster_id directly
        -- (no BR member to anchor to). Provider int models coalesce against
        -- these so cluster members at every grain share the same canonical
        -- gp_*_id and collapse to one mart row in the BR + TS + DDHQ FOJ.
        --
        -- Salts per grain ensure each grain's canonical is distinct but
        -- deterministic. cluster_id is unique per cluster so cluster members
        -- share the same canonicals; different clusters get different IDs
        -- (cross-cluster merging at election/race grain still relies on
        -- providers' natural deterministic hashes — same as today).
        --
        -- Dedup on each provider's natural key (matches the BR-anchored
        -- branches' partition keys). Two TS records sharing the same stripped
        -- (source_candidate_id, election_date) but in different non-BR
        -- clusters keep one canonical, deterministically by min(cluster_id).
        select
            regexp_replace(
                cw.source_id, '__(primary|general|runoff)$', ''
            ) as ts_source_candidate_id,
            cast(cw.election_date as date) as ts_stage_election_date,
            cast(null as bigint) as gp_api_campaign_id,
            cast(null as date) as gp_api_stage_election_date,
            cast(null as bigint) as ddhq_candidate_id,
            cast(null as bigint) as ddhq_race_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy_stage"
                )
            }} as canonical_gp_candidacy_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_election_stage"
                )
            }} as canonical_gp_election_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy"
                )
            }} as canonical_gp_candidacy_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidate"
                )
            }} as canonical_gp_candidate_id,
            {{ generate_salted_uuid(fields=["cw.cluster_id"], salt="non_br_election") }}
            as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
        where cw.source_name = 'techspeed'
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
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy_stage"
                )
            }} as canonical_gp_candidacy_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_election_stage"
                )
            }} as canonical_gp_election_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy"
                )
            }} as canonical_gp_candidacy_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidate"
                )
            }} as canonical_gp_candidate_id,
            {{ generate_salted_uuid(fields=["cw.cluster_id"], salt="non_br_election") }}
            as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
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
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy_stage"
                )
            }} as canonical_gp_candidacy_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_election_stage"
                )
            }} as canonical_gp_election_stage_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidacy"
                )
            }} as canonical_gp_candidacy_id,
            {{
                generate_salted_uuid(
                    fields=["cw.cluster_id"], salt="non_br_candidate"
                )
            }} as canonical_gp_candidate_id,
            {{ generate_salted_uuid(fields=["cw.cluster_id"], salt="non_br_election") }}
            as canonical_gp_election_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
        inner join non_br_clusters using (cluster_id)
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
    canonical_gp_candidate_id,
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
    canonical_gp_candidate_id,
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
    canonical_gp_candidate_id,
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
    canonical_gp_candidate_id,
    canonical_gp_election_id
from non_br_cluster_matches
