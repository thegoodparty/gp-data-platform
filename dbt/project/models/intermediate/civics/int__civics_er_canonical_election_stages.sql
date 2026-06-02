-- Direct election-stage entity-resolution crosswalk: maps each DDHQ / TechSpeed
-- election stage that clustered with a BallotReady race to BR's canonical
-- gp_election_stage_id. Sourced from the gp-data-matcha election_stage entity,
-- which clusters races by office + geography + election cycle, so it links
-- races even when no candidacy matched across sources -- broader coverage than
-- the candidacy-derived canonical_gp_election_stage_id in
-- int__civics_er_canonical_ids.
--
-- BR-anchored clusters only. Non-BR clusters (DDHQ/TS with no BR member) are
-- intentionally omitted: per non_br_cluster_canonicals, a cluster-derived
-- election_stage canonical has no BR natural id to anchor to and conflicts with
-- BR's natural id downstream (e.g. m_election_api joins on
-- br_position_database_id), duplicating rows. Those records keep their natural
-- gp_election_stage_id.
--
-- Grain: one row per non-BR provider election stage (source_name +
-- gp_election_stage_id). Keyed on the cluster table's source_id (the provider's
-- own gp_election_stage_id) rather than a ref to the provider model, so the
-- consuming DDHQ / TS election_stage models can coalesce against this crosswalk
-- without creating a dbt cycle.
with
    clusters as (select * from {{ ref("stg_er_source__clustered_election_stages") }}),

    -- One canonical BR gp_election_stage_id per cluster (the BR member's id).
    br_anchor as (
        select
            clusters.cluster_id,
            br_es.gp_election_stage_id as canonical_gp_election_stage_id
        from clusters
        inner join
            {{ ref("int__civics_election_stage_ballotready") }} as br_es
            on clusters.source_id = br_es.br_race_id
        where clusters.source_name = 'ballotready'
        qualify
            row_number() over (
                partition by clusters.cluster_id order by br_es.gp_election_stage_id
            )
            = 1
    )

select
    clusters.source_name,
    -- The provider's own gp_election_stage_id (cluster source_id); consumers
    -- coalesce their gp_election_stage_id against canonical_gp_election_stage_id.
    clusters.source_id as gp_election_stage_id,
    br_anchor.canonical_gp_election_stage_id
from clusters
inner join br_anchor using (cluster_id)
where clusters.source_name in ('ddhq', 'techspeed')
