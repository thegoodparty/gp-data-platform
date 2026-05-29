-- A4: warn-only signal for same-(ts_code, election_date) mixed-cluster
-- collisions in the matcha output.
--
-- DATA-1523 Phase 3 tactical trade-off: when matcha produces multiple
-- clusters for the same stripped ts_source_candidate_id on the same
-- election_date (e.g. a TS person filed for two different specialty
-- districts in the same town on the same day), int__civics_er_canonical_ids's
-- non_br_cluster_matches branch dedupes against ts_stage_matches so
-- ts_key_unique_in_crosswalk stays green. The TS-only candidacy then
-- collapses under the BR canonical gp_candidacy_id in
-- int__civics_candidacy_techspeed's dedupe and is absent from
-- mart_civics.candidacy. Known case at PR-418 time: Robert Emmons in
-- Kennebunk ME 2026-06-09 (Wells Water District General, BR-paired;
-- Kennebunk Sewer District Primary, TS-only). The Sewer Primary is
-- silently dropped.
--
-- Root cause is upstream: the TS candidate_code is keyed on
-- (first_name, last_name, state, city, office_type) and office_type='other'
-- can't distinguish specialty districts in the same city. Proper fix
-- (add official_office_name to candidate_code) would re-rotate codes for
-- many TS rows — a Phase-2-scale change to schedule deliberately.
--
-- Calibrated to current observed count at PR-418 time so the test
-- warns on GROWTH past today's known set, not on the known set itself
-- (the follow-up ticket tracks the resolution path). Today's known
-- collision pairs (6 total in the 2026-05-27 matcha output):
-- robert__emmons__maine__kennebunk__other (Water vs Sewer)
-- jerry__taverna__massachusetts__hull__town-council (Planning vs Select)
-- susan__short-green__massachusetts__hull__town-council
-- jason__mccann__massachusetts__hull__town-council
-- brian__massey__vermont__northfield__town-council
-- huey__eason__georgia__evans-county__school-board
--
-- All six are same-person-multi-office-same-date in small towns where
-- office_type='other' or 'town-council' collapses different bodies.
-- >20 cases would indicate the architectural fix is now urgent.
{{ config(severity="warn", warn_if="> 6", error_if="> 20") }}

with
    ts_keys as (
        select
            regexp_replace(source_id, '__(primary|general|runoff)$', '') as ts_code,
            cast(election_date as date) as election_date,
            cluster_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
        where source_name = 'techspeed' and election_date is not null
    )

select ts_code, election_date, count(distinct cluster_id) as n_clusters
from ts_keys
group by ts_code, election_date
having count(distinct cluster_id) > 1
