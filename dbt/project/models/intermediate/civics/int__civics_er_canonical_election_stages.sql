-- Direct election-stage entity-resolution crosswalk: maps each DDHQ / TechSpeed
-- election stage to a canonical gp_election_stage_id for its cluster. Sourced
-- from the gp-data-matcha election_stage entity, which clusters races by office
-- + geography + election cycle, so it links races even when no candidacy
-- matched across sources -- broader coverage than the candidacy-derived
-- canonical_gp_election_stage_id in int__civics_er_canonical_ids.
--
-- Grain: one row per non-BR provider election stage (source_name +
-- gp_election_stage_id). Keyed on the cluster table's source_id (the provider's
-- own gp_election_stage_id) rather than a ref to the provider model, so the
-- consuming DDHQ / TS election_stage models can coalesce against this crosswalk
-- without creating a dbt cycle.
with
    clusters as (select * from {{ ref("stg_er_source__clustered_election_stages") }}),

    -- One canonical BR gp_election_stage_id per BR-anchored cluster.
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
    ),

    -- Multi-record clusters with no BR member: pick a deterministic
    -- representative (prefer DDHQ's id over TS, then lowest id) as the canonical
    -- so the cross-source DDHQ <-> TS match collapses to one stage.
    non_br_anchor as (
        select clusters.cluster_id, clusters.source_id as canonical_gp_election_stage_id
        from clusters
        inner join
            (
                select cluster_id
                from clusters
                group by cluster_id
                having count_if(source_name = 'ballotready') = 0 and count(*) > 1
            ) as non_br_clusters using (cluster_id)
        qualify
            row_number() over (
                partition by clusters.cluster_id
                order by (clusters.source_name = 'ddhq') desc, clusters.source_id
            )
            = 1
    )

-- BR-anchored: DDHQ/TS members adopt the BR race's canonical.
select
    clusters.source_name,
    clusters.source_id as gp_election_stage_id,
    br_anchor.canonical_gp_election_stage_id
from clusters
inner join br_anchor using (cluster_id)
where clusters.source_name in ('ddhq', 'techspeed')

union all

-- Non-BR cross-source: every member adopts the cluster's representative id
-- (the representative maps to itself, which is harmless for the coalesce).
select
    clusters.source_name,
    clusters.source_id as gp_election_stage_id,
    non_br_anchor.canonical_gp_election_stage_id
from clusters
inner join non_br_anchor using (cluster_id)
where clusters.source_name in ('ddhq', 'techspeed')
