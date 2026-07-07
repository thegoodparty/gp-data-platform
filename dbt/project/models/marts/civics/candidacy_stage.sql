-- Civics mart candidacy_stage table.
--
-- Two eras (see canonical-person-plan.md decision 4):
-- 2026+  : 4-way cluster-based FOJ over BR + TS + DDHQ + gp_api. Byte-stable
-- vs the pre-person mart; gp_person_id is the only added column.
-- <=2025 : the HubSpot archive is the spine. BR / DDHQ / TS candidacy-stage
-- rows for the same person-stage merge into the archive row with
-- HubSpot-first display precedence (HubSpot > DDHQ > BR > TS).
-- HubSpot records are not in candidacy-stage ER, so the person layer
-- (gp_person_id) is the bridge: archive rows resolve their person via
-- their HubSpot contact, FOJ rows via their native vendor keys, and
-- the two sides merge on (gp_person_id, election_stage_date,
-- election_stage). The era merge is contained to this mart: the BR
-- and DDHQ candidacy/election/election_stage/candidate intermediates
-- stay 2026-gated, so an archive-uncovered <=2025 FOJ row survives
-- standalone only when both its gp_candidacy_id and
-- gp_election_stage_id are real mart FKs (TechSpeed qualifies as
-- today; BR / DDHQ <=2025 rows only ever enrich an archive row).
--
-- gp_person_id is the deterministic min over the person ids reached by a row's
-- native keys; cluster-merged rows are one person by construction, the min is a
-- guard (warn test in m_civics.yaml).
{%- set gp_api_wins_cols = [
    "candidate_name",
    "source_candidate_id",
    "candidate_party",
    "election_stage_date",
    "created_at",
    "updated_at",
] %}
{%- set ddhq_wins_cols = [
    "is_winner",
    "election_result",
    "election_result_source",
    "votes_received",
] %}

with
    -- record_key -> gp_person_id. One row per record_key, so every join below
    -- is fan-out-safe.
    person_ids as (
        select record_key, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    -- br_candidacy_id -> person (via br_candidate_id, the BR person grain).
    br_candidacy_person as (
        select distinct
            cast(c.br_candidacy_id as string) as br_candidacy_id, p.gp_person_id
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as c
        inner join
            person_ids as p
            on p.record_key = 'ballotready|' || cast(c.br_candidate_id as string)
        where c.br_candidacy_id is not null
    ),

    -- Stage-stripped TS code -> person. E7 collapses a vendor person's
    -- primary/general split, so a code maps to one person (min guards residue).
    ts_code_person as (
        select
            {{ strip_ts_stage_suffix("substring_index(record_key, '|', -1)") }}
            as ts_code,
            min(gp_person_id) as gp_person_id
        from person_ids
        where record_key like 'techspeed|%'
        group by 1
    ),

    -- gp_api campaign -> user -> person.
    gp_api_campaign_person as (
        select cast(camp.campaign_id as string) as campaign_id, p.gp_person_id
        from {{ ref("campaigns") }} as camp
        inner join
            person_ids as p on p.record_key = 'gp_api|' || cast(camp.user_id as string)
        where camp.is_latest_version and camp.user_id is not null
    ),

    -- Archive candidacy -> HubSpot contact, for the archive person link.
    archive_contact as (
        select gp_candidacy_id, cast(hubspot_contact_id as string) as hubspot_contact_id
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Every real candidacy id across the marts, without referencing the
    -- candidacy mart itself (which reads this model). Gates archive-uncovered
    -- <=2025 FOJ rows to those with a valid candidacy FK.
    valid_candidacy_ids as (
        select gp_candidacy_id
        from {{ ref("int__civics_candidacy_ballotready") }}
        union
        select gp_candidacy_id
        from {{ ref("int__civics_candidacy_ddhq") }}
        union
        select gp_candidacy_id
        from {{ ref("int__civics_candidacy_techspeed") }}
        union
        select gp_candidacy_id
        from {{ ref("int__civics_candidacy_gp_api") }}
        union
        select gp_candidacy_id
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Valid election-stage FKs. BR <=2025 stages are absent here (the BR
    -- election_stage intermediate stays 2026-gated), so this drops BR-only
    -- <=2025 rows that slipped past the candidacy gate on a shared id hash;
    -- their BR-derived gp_election_stage_id would otherwise be an orphan FK.
    valid_election_stage_ids as (
        select gp_election_stage_id from {{ ref("election_stage") }}
    ),

    archive_2025 as (
        select
            s.gp_candidacy_stage_id,
            s.gp_candidacy_id,
            s.gp_election_stage_id,
            s.candidate_name,
            cast(s.source_candidate_id as string) as source_candidate_id,
            s.candidate_party,
            s.election_stage_date,
            s.created_at,
            s.updated_at,
            s.stage_type as election_stage,
            cast(s.source_race_id as string) as source_race_id,
            s.is_winner,
            s.election_result,
            s.election_result_source,
            s.votes_received,
            cast(s.match_confidence as float) as match_confidence,
            s.match_reasoning,
            s.match_top_candidates,
            s.has_match,
            cast(null as boolean) as is_uncontested,
            array_compact(
                array('hubspot', case when s.has_match then 'ddhq' end)
            ) as source_systems,
            cast(null as string) as er_cluster_id,
            cast(null as string) as br_candidacy_id,
            cast(null as string) as ts_source_candidate_id,
            cast(null as string) as gp_api_campaign_id,
            case
                when s.has_match then cast(s.source_candidate_id as string)
            end as ddhq_candidate_id,
            case
                when s.has_match then cast(s.source_race_id as string)
            end as ddhq_race_id,
            pi_hs.gp_person_id
        from {{ ref("int__civics_candidacy_stage_2025") }} as s
        left join archive_contact as ac on ac.gp_candidacy_id = s.gp_candidacy_id
        left join
            person_ids as pi_hs
            on pi_hs.record_key = 'hubspot|' || ac.hubspot_contact_id
    ),

    -- DDHQ projected into the BR/TS merge-row schema (column rename only).
    ddhq as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_full_name as candidate_name,
            source_candidate_id,
            source_race_id,
            party_affiliation as candidate_party,
            election_date as election_stage_date,
            election_stage,
            created_at,
            updated_at,
            is_winner,
            election_result,
            election_result_source,
            cast(votes as string) as votes_received,
            is_uncontested
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
    ),

    br_with_cluster as (
        select
            br.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || br.gp_candidacy_stage_id) as merge_key
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as br
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'ballotready'
            and cw.br_candidacy_id = br.br_candidacy_id
    ),

    ts_with_cluster as (
        select
            ts.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || ts.gp_candidacy_stage_id) as merge_key
        from {{ ref("int__civics_candidacy_stage_techspeed") }} as ts
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'techspeed'
            and {{ strip_ts_stage_suffix("cw.source_id") }} = ts.source_candidate_id
            and cw.election_date = ts.election_stage_date
    ),

    ddhq_with_cluster as (
        select
            ddhq.*,
            cw.cluster_id,
            coalesce(cw.cluster_id, 'self_' || ddhq.gp_candidacy_stage_id) as merge_key
        from ddhq
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'ddhq'
            and cw.source_id = ddhq.source_candidate_id || '_' || ddhq.source_race_id
    ),

    gp_api_with_cluster as (
        select
            gp_api.*,
            cw.cluster_id,
            coalesce(
                cw.cluster_id, 'self_' || gp_api.gp_candidacy_stage_id
            ) as merge_key
        from {{ ref("int__civics_candidacy_stage_gp_api") }} as gp_api
        left join
            {{ ref("stg_er_source__clustered_candidacy_stages") }} as cw
            on cw.source_name = 'gp_api'
            and split(cw.source_id, '__')[0] = gp_api.source_candidate_id
            and cw.election_date = gp_api.election_stage_date
    ),

    merged_foj as (
        select
            coalesce(
                gp_api.gp_candidacy_stage_id,
                br.gp_candidacy_stage_id,
                ts.gp_candidacy_stage_id,
                ddhq.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            coalesce(
                gp_api.gp_candidacy_id,
                br.gp_candidacy_id,
                ts.gp_candidacy_id,
                ddhq.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                gp_api.gp_election_stage_id,
                br.gp_election_stage_id,
                ts.gp_election_stage_id,
                ddhq.gp_election_stage_id
            ) as gp_election_stage_id,
            {% for col in gp_api_wins_cols %}
                coalesce(
                    gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}
                ) as {{ col }},
            {% endfor %}
            -- Native BR/DDHQ stage so <=2025 rows (absent from election_stage)
            -- still carry a stage for the person-stage merge.
            coalesce(br.election_stage, ddhq.election_stage) as native_stage,
            coalesce(
                br.source_race_id, ts.source_race_id, ddhq.source_race_id
            ) as source_race_id,
            {% for col in ddhq_wins_cols %}
                coalesce(ddhq.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            coalesce(br.match_confidence, ts.match_confidence) as match_confidence,
            coalesce(br.match_reasoning, ts.match_reasoning) as match_reasoning,
            coalesce(
                br.match_top_candidates, ts.match_top_candidates
            ) as match_top_candidates,
            coalesce(br.has_match, ts.has_match) as has_match,
            ddhq.is_uncontested,
            array_compact(
                array(
                    case when br.merge_key is not null then 'ballotready' end,
                    case when ts.merge_key is not null then 'techspeed' end,
                    case when ddhq.merge_key is not null then 'ddhq' end,
                    case when gp_api.merge_key is not null then 'gp_api' end
                )
            ) as source_systems,
            coalesce(
                br.cluster_id, ts.cluster_id, ddhq.cluster_id, gp_api.cluster_id
            ) as er_cluster_id,
            br.br_candidacy_id,
            ts.source_candidate_id as ts_source_candidate_id,
            gp_api.source_candidate_id as gp_api_campaign_id,
            ddhq.source_candidate_id as ddhq_candidate_id,
            ddhq.source_race_id as ddhq_race_id
        from br_with_cluster as br
        full outer join ts_with_cluster as ts on br.merge_key = ts.merge_key
        full outer join
            ddhq_with_cluster as ddhq
            on coalesce(br.merge_key, ts.merge_key) = ddhq.merge_key
        full outer join
            gp_api_with_cluster as gp_api
            on coalesce(br.merge_key, ts.merge_key, ddhq.merge_key) = gp_api.merge_key
    ),

    -- Resolve gp_person_id (deterministic min over the person ids each native
    -- key reaches) and the final election_stage (2026+ keeps election_stage's
    -- stage_type for byte-stability; <=2025 falls back to the native stage).
    foj as (
        select
            m.gp_candidacy_stage_id,
            m.gp_candidacy_id,
            m.gp_election_stage_id,
            m.candidate_name,
            m.source_candidate_id,
            m.candidate_party,
            m.election_stage_date,
            m.created_at,
            m.updated_at,
            coalesce(es.stage_type, m.native_stage) as election_stage,
            m.source_race_id,
            m.is_winner,
            m.election_result,
            m.election_result_source,
            m.votes_received,
            m.match_confidence,
            m.match_reasoning,
            m.match_top_candidates,
            m.has_match,
            m.is_uncontested,
            m.source_systems,
            m.er_cluster_id,
            m.br_candidacy_id,
            m.ts_source_candidate_id,
            m.gp_api_campaign_id,
            m.ddhq_candidate_id,
            m.ddhq_race_id,
            least(
                bcp.gp_person_id, tcp.gp_person_id, gcp.gp_person_id, dp.gp_person_id
            ) as gp_person_id
        from merged_foj as m
        left join
            {{ ref("election_stage") }} as es
            on m.gp_election_stage_id = es.gp_election_stage_id
        left join br_candidacy_person as bcp on bcp.br_candidacy_id = m.br_candidacy_id
        left join ts_code_person as tcp on tcp.ts_code = m.ts_source_candidate_id
        left join
            gp_api_campaign_person as gcp on gcp.campaign_id = m.gp_api_campaign_id
        left join
            person_ids as dp
            on dp.record_key = 'ddhq|' || m.ddhq_candidate_id || '_' || m.ddhq_race_id
    ),

    -- Passthrough era: byte-stable 2026+ (plus any null-date rows, unchanged).
    foj_passthrough as (
        select *
        from foj
        where election_stage_date >= '2026-01-01' or election_stage_date is null
    ),

    foj_archive_era as (select * from foj where election_stage_date <= '2025-12-31'),

    -- One representative FOJ row per archive person-stage (prefer a resolved
    -- outcome, DDHQ then BR then TS), plus the union of all its FOJ sources.
    foj_best as (
        select
            *,
            array_distinct(
                flatten(
                    collect_list(source_systems) over (
                        partition by gp_person_id, election_stage_date, election_stage
                    )
                )
            ) as ps_sources
        from foj_archive_era
        where gp_person_id is not null
        qualify
            row_number() over (
                partition by gp_person_id, election_stage_date, election_stage
                order by
                    (election_result is not null) desc,
                    array_contains(source_systems, 'ddhq') desc,
                    array_contains(source_systems, 'ballotready') desc,
                    gp_candidacy_stage_id
            )
            = 1
    ),

    -- Archive rows enriched with the FOJ person-stage: HubSpot-first display,
    -- archive ids preserved, source_systems unioned.
    archive_enriched as (
        select
            a.gp_candidacy_stage_id,
            a.gp_candidacy_id,
            a.gp_election_stage_id,
            coalesce(a.candidate_name, f.candidate_name) as candidate_name,
            a.source_candidate_id,
            a.candidate_party,
            a.election_stage_date,
            a.created_at,
            a.updated_at,
            a.election_stage,
            a.source_race_id,
            coalesce(a.is_winner, f.is_winner) as is_winner,
            coalesce(a.election_result, f.election_result) as election_result,
            coalesce(
                a.election_result_source, f.election_result_source
            ) as election_result_source,
            coalesce(a.votes_received, f.votes_received) as votes_received,
            a.match_confidence,
            a.match_reasoning,
            a.match_top_candidates,
            coalesce(a.has_match, f.has_match) as has_match,
            coalesce(a.is_uncontested, f.is_uncontested) as is_uncontested,
            case
                when f.gp_person_id is not null
                then array_union(a.source_systems, f.ps_sources)
                else a.source_systems
            end as source_systems,
            coalesce(a.er_cluster_id, f.er_cluster_id) as er_cluster_id,
            coalesce(a.br_candidacy_id, f.br_candidacy_id) as br_candidacy_id,
            coalesce(
                a.ts_source_candidate_id, f.ts_source_candidate_id
            ) as ts_source_candidate_id,
            coalesce(a.gp_api_campaign_id, f.gp_api_campaign_id) as gp_api_campaign_id,
            coalesce(a.ddhq_candidate_id, f.ddhq_candidate_id) as ddhq_candidate_id,
            coalesce(a.ddhq_race_id, f.ddhq_race_id) as ddhq_race_id,
            a.gp_person_id
        from archive_2025 as a
        left join
            foj_best as f
            on a.gp_person_id = f.gp_person_id
            and a.election_stage_date = f.election_stage_date
            and a.election_stage = f.election_stage
    ),

    -- Person-stages already occupied by an archive row.
    covered_pstages as (
        select distinct gp_person_id, election_stage_date, election_stage
        from archive_2025
        where gp_person_id is not null
    ),

    -- <=2025 FOJ rows with no archive counterpart survive only with a valid
    -- candidacy FK (drops orphan BR-only rows; keeps DDHQ / TS coverage).
    foj_standalone as (
        select f.*
        from foj_archive_era as f
        left join
            covered_pstages as c
            on f.gp_person_id = c.gp_person_id
            and f.election_stage_date = c.election_stage_date
            and f.election_stage = c.election_stage
        where
            c.gp_person_id is null
            and f.gp_candidacy_id in (select gp_candidacy_id from valid_candidacy_ids)
            and (
                f.gp_election_stage_id is null
                or f.gp_election_stage_id
                in (select gp_election_stage_id from valid_election_stage_ids)
            )
    ),

    combined as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_name,
            source_candidate_id,
            source_race_id,
            candidate_party,
            is_winner,
            election_result,
            election_result_source,
            match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            votes_received,
            is_uncontested,
            election_stage_date,
            election_stage,
            source_systems,
            er_cluster_id,
            br_candidacy_id,
            ts_source_candidate_id,
            gp_api_campaign_id,
            ddhq_candidate_id,
            ddhq_race_id,
            gp_person_id,
            created_at,
            updated_at
        from archive_enriched
        union all
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_name,
            source_candidate_id,
            source_race_id,
            candidate_party,
            is_winner,
            election_result,
            election_result_source,
            match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            votes_received,
            is_uncontested,
            election_stage_date,
            election_stage,
            source_systems,
            er_cluster_id,
            br_candidacy_id,
            ts_source_candidate_id,
            gp_api_campaign_id,
            ddhq_candidate_id,
            ddhq_race_id,
            gp_person_id,
            created_at,
            updated_at
        from foj_standalone
        union all
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            candidate_name,
            source_candidate_id,
            source_race_id,
            candidate_party,
            is_winner,
            election_result,
            election_result_source,
            match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            votes_received,
            is_uncontested,
            election_stage_date,
            election_stage,
            source_systems,
            er_cluster_id,
            br_candidacy_id,
            ts_source_candidate_id,
            gp_api_campaign_id,
            ddhq_candidate_id,
            ddhq_race_id,
            gp_person_id,
            created_at,
            updated_at
        from foj_passthrough
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc nulls last
            )
            = 1
    )

select
    deduplicated.gp_candidacy_stage_id,
    deduplicated.gp_candidacy_id,
    deduplicated.gp_election_stage_id,
    deduplicated.gp_person_id,
    deduplicated.candidate_name,
    deduplicated.source_candidate_id,
    deduplicated.source_race_id,
    deduplicated.candidate_party,
    deduplicated.is_winner,
    deduplicated.election_result,
    deduplicated.election_result_source,
    deduplicated.match_confidence,
    deduplicated.match_reasoning,
    deduplicated.match_top_candidates,
    deduplicated.has_match,
    deduplicated.votes_received,
    deduplicated.is_uncontested,
    deduplicated.election_stage_date,
    deduplicated.election_stage,
    es.is_win_icp,
    es.is_serve_icp,
    es.is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.er_cluster_id,
    deduplicated.br_candidacy_id,
    deduplicated.ts_source_candidate_id,
    deduplicated.gp_api_campaign_id,
    deduplicated.ddhq_candidate_id,
    deduplicated.ddhq_race_id,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("election_stage") }} as es
    on deduplicated.gp_election_stage_id = es.gp_election_stage_id
