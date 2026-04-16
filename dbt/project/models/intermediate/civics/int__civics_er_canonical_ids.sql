-- Entity resolution crosswalk: TechSpeed raw keys → BallotReady canonical gp_* IDs.
--
-- For each TS candidacy_stage that is clustered with a BR candidacy_stage, this
-- model emits BR's gp_* IDs at every level (stage, candidacy, candidate,
-- election_stage, election). TS intermediate models left-join this on raw keys
-- and coalesce over their own hashes, so clustered TS rows naturally share IDs
-- with their BR counterparts. The civics marts then become uniform
-- full-outer-join + coalesce patterns with no remap CTEs.
--
-- Grain: one row per matched (ts_source_candidate_id, ts_stage_election_date).
-- Keyed on raw TS fields (not on TS-computed hashes) to avoid a cycle with the
-- TS intermediate models that consume this crosswalk.
with
    stage_matches as (
        select
            regexp_replace(
                ts_cw.source_id, '__(primary|general|runoff)$', ''
            ) as ts_source_candidate_id,
            cast(ts_cw.election_date as date) as ts_stage_election_date,
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
    )

-- Dedupe: keep the most recent BR match per TS stage key
select
    ts_source_candidate_id,
    ts_stage_election_date,
    canonical_gp_candidacy_stage_id,
    canonical_gp_election_stage_id,
    canonical_gp_candidacy_id,
    canonical_gp_candidate_id,
    canonical_gp_election_id
from stage_matches
qualify
    row_number() over (
        partition by ts_source_candidate_id, ts_stage_election_date
        order by br_updated_at desc
    )
    = 1
