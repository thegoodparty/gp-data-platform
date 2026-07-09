-- Civics mart candidacy_stage table.
--
-- Two eras:
-- 2026+  : 4-way cluster-based FOJ over BR + TS + DDHQ + gp_api. Byte-stable
-- vs the pre-person mart; gp_person_id is the only added column.
-- <=2025 : the HubSpot archive is the spine (it is the outcome authority for
-- pre-2026 races). BR / DDHQ / TS rows for the same person-stage
-- merge into the archive row with HubSpot-first display precedence
-- (HubSpot > DDHQ > BR > TS); HubSpot only records general-stage
-- outcomes, so primaries legitimately display DDHQ. HubSpot records
-- are not in candidacy-stage ER, so the person layer (gp_person_id)
-- is the bridge: archive rows resolve their person via their HubSpot
-- contact, FOJ rows via their native vendor keys, and the two sides
-- pair on (gp_person_id, election_stage_date, election_stage).
-- Archive-uncovered FOJ rows survive standalone only with FK-valid
-- ids: each row adopts the first provider id trio (merged coalesce,
-- then TS, then DDHQ) whose candidacy / election-stage ids exist in
-- the marts. The BR and DDHQ candidacy, election, election_stage,
-- and candidate intermediates stay 2026-gated (their all-time
-- expansion is a later PR), so today only TS-derived <=2025 ids are
-- valid: a BR- or DDHQ-keyed <=2025 row either enriches an archive
-- row, rides a TS-keyed merged row, or is dropped.
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
{%- set out_cols = [
    "gp_candidacy_stage_id",
    "gp_candidacy_id",
    "gp_election_stage_id",
    "candidate_name",
    "source_candidate_id",
    "source_race_id",
    "candidate_party",
    "is_winner",
    "election_result",
    "election_result_source",
    "match_confidence",
    "match_reasoning",
    "match_top_candidates",
    "has_match",
    "votes_received",
    "is_uncontested",
    "election_stage_date",
    "election_stage",
    "source_systems",
    "er_cluster_id",
    "br_candidacy_id",
    "ts_source_candidate_id",
    "gp_api_campaign_id",
    "ddhq_candidate_id",
    "ddhq_race_id",
    "gp_person_id",
    "created_at",
    "updated_at",
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
    -- candidacy mart itself (which reads this model).
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
            -- still carry a stage for the person-stage merge. DDHQ's special-
            -- election variants have no counterpart in the archive's stage_type
            -- (primary/general/general runoff only), so they're normalized to
            -- their non-special base, preserving the primary/general
            -- distinction. BR is unaffected: it stays 2026-gated, so
            -- br.election_stage is always null on an archive-era row.
            coalesce(
                br.election_stage,
                case
                    when ddhq.election_stage = 'primary special'
                    then 'primary'
                    when ddhq.election_stage = 'general special'
                    then 'general'
                    when ddhq.election_stage = 'general special runoff'
                    then 'general runoff'
                    when ddhq.election_stage = 'primary special runoff'
                    then 'primary runoff'
                    else ddhq.election_stage
                end
            ) as native_stage,
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
            ddhq.source_race_id as ddhq_race_id,
            -- Per-provider id trios: <=2025 standalone rows adopt the first
            -- trio whose FKs are valid (the merged coalesce prefers BR, whose
            -- <=2025 ids are not in the 2026-gated BR candidacy/election_stage
            -- marts -- without the fallback a BR+TS cluster's row would be
            -- silently dropped, destroying TS coverage that exists today).
            ts.gp_candidacy_stage_id as ts_gp_candidacy_stage_id,
            ts.gp_candidacy_id as ts_gp_candidacy_id,
            ts.gp_election_stage_id as ts_gp_election_stage_id,
            ddhq.gp_candidacy_stage_id as ddhq_gp_candidacy_stage_id,
            ddhq.gp_candidacy_id as ddhq_gp_candidacy_id,
            ddhq.gp_election_stage_id as ddhq_gp_election_stage_id
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
            m.*,
            coalesce(es.stage_type, m.native_stage) as election_stage,
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

    -- Person-linked FOJ rows, each carrying the union of every source seen at
    -- its person-stage (the merged archive row credits all contributors even
    -- when only one FOJ row supplies the display fields).
    foj_person_stage as (
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
    ),

    -- Archive rows enriched with their best-matching FOJ row: HubSpot-first
    -- display, archive ids preserved, source_systems unioned. Paired per
    -- archive row so same-day multi-office people don't cross-contaminate:
    -- when the archive row has a DDHQ match, an FOJ row for the same DDHQ race
    -- outranks everything, then resolved outcomes, then DDHQ > BR.
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
            a.gp_person_id,
            f.gp_candidacy_stage_id as matched_foj_pk
        from archive_2025 as a
        left join
            foj_person_stage as f
            on a.gp_person_id = f.gp_person_id
            and a.election_stage_date = f.election_stage_date
            and a.election_stage = f.election_stage
        qualify
            row_number() over (
                partition by a.gp_candidacy_stage_id
                order by
                    (
                        f.ddhq_race_id is not null and f.ddhq_race_id <=> a.ddhq_race_id
                    ) desc,
                    (f.election_result is not null) desc,
                    array_contains(f.source_systems, 'ddhq') desc,
                    array_contains(f.source_systems, 'ballotready') desc,
                    f.gp_candidacy_stage_id
            )
            = 1
    ),

    -- FOJ rows consumed as archive enrichment. Suppression is scoped to the
    -- matched row (not the whole person-stage) so a same-day row for a
    -- genuinely different office survives standalone.
    consumed_foj as (
        select distinct matched_foj_pk
        from archive_enriched
        where matched_foj_pk is not null
    ),

    -- Archive-uncovered <=2025 FOJ rows. Each adopts the first id trio with
    -- valid FKs: merged coalesce, then TS, then DDHQ. No valid trio -> dropped
    -- (BR-only rows; BR's <=2025 ids are not in the 2026-gated BR marts).
    foj_standalone as (
        select
            f.* except (gp_candidacy_stage_id, gp_candidacy_id, gp_election_stage_id),
            vc_o.gp_candidacy_id is not null
            and (
                f.gp_election_stage_id is null or ve_o.gp_election_stage_id is not null
            ) as orig_valid,
            vc_t.gp_candidacy_id is not null
            and (
                f.ts_gp_election_stage_id is null
                or ve_t.gp_election_stage_id is not null
            ) as ts_valid,
            vc_d.gp_candidacy_id is not null
            and (
                f.ddhq_gp_election_stage_id is null
                or ve_d.gp_election_stage_id is not null
            ) as ddhq_valid,
            case
                when orig_valid
                then f.gp_candidacy_stage_id
                when ts_valid
                then f.ts_gp_candidacy_stage_id
                when ddhq_valid
                then f.ddhq_gp_candidacy_stage_id
            end as gp_candidacy_stage_id,
            case
                when orig_valid
                then f.gp_candidacy_id
                when ts_valid
                then f.ts_gp_candidacy_id
                when ddhq_valid
                then f.ddhq_gp_candidacy_id
            end as gp_candidacy_id,
            case
                when orig_valid
                then f.gp_election_stage_id
                when ts_valid
                then f.ts_gp_election_stage_id
                when ddhq_valid
                then f.ddhq_gp_election_stage_id
            end as gp_election_stage_id
        from foj_archive_era as f
        left join consumed_foj as cf on cf.matched_foj_pk = f.gp_candidacy_stage_id
        left join
            valid_candidacy_ids as vc_o on vc_o.gp_candidacy_id = f.gp_candidacy_id
        left join
            valid_election_stage_ids as ve_o
            on ve_o.gp_election_stage_id = f.gp_election_stage_id
        left join
            valid_candidacy_ids as vc_t on vc_t.gp_candidacy_id = f.ts_gp_candidacy_id
        left join
            valid_election_stage_ids as ve_t
            on ve_t.gp_election_stage_id = f.ts_gp_election_stage_id
        left join
            valid_candidacy_ids as vc_d on vc_d.gp_candidacy_id = f.ddhq_gp_candidacy_id
        left join
            valid_election_stage_ids as ve_d
            on ve_d.gp_election_stage_id = f.ddhq_gp_election_stage_id
        where cf.matched_foj_pk is null
    ),

    combined as (
        select {{ out_cols | join(", ") }}
        from archive_enriched
        union all
        select {{ out_cols | join(", ") }}
        from foj_standalone
        where gp_candidacy_stage_id is not null
        union all
        select {{ out_cols | join(", ") }}
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
