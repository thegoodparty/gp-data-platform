-- Civics mart candidacy table.
-- 2025 HubSpot archive UNION 2026+ 4-way FOJ over BR + TS + DDHQ + gp_api,
-- joined on gp_candidacy_id (matched providers adopt BR's canonical via
-- int__civics_er_canonical_ids). Per-column precedence rules: see the
-- candidacy model description in m_civics.yaml.
{%- set gp_api_wins_cols = [
    "hubspot_contact_id",
    "candidate_id_source",
    "party_affiliation",
    "candidate_office",
    "official_office_name",
    "office_level",
    "is_partisan",
    "primary_election_date",
    "primary_runoff_election_date",
    "general_election_date",
    "general_runoff_election_date",
    "created_at",
    "updated_at",
] %}
{# Subset of gp_api_wins_cols that DDHQ also supplies. The loop adds DDHQ
   to the coalesce chain only for cols listed here; cols in gp_api_wins_cols
   but NOT here render a 3-way coalesce(gp_api, br, ts) instead. #}
{%- set ddhq_fallback_cols = [
    "candidate_id_source",
    "party_affiliation",
    "candidate_office",
    "official_office_name",
    "office_level",
    "general_election_date",
    "created_at",
    "updated_at",
] %}

with
    archive_2025 as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            -- Column order must match merged_since_2026
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            score_viability_automated,
            array_compact(
                array('hubspot', case when has_ddhq_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Four-way FOJ. TS / DDHQ / gp_api int models all remap clustered rows
    -- to BR's gp_candidacy_id via int__civics_er_canonical_ids, so a FOJ on
    -- gp_candidacy_id auto-merges matched quadruples. Unmatched rows on any
    -- side pass through with NULLs on absent providers.
    merged_since_2026 as (
        select
            coalesce(
                gp_api.gp_candidacy_id,
                br.gp_candidacy_id,
                ts.gp_candidacy_id,
                ddhq.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                gp_api.gp_candidate_id,
                br.gp_candidate_id,
                ts.gp_candidate_id,
                ddhq.gp_candidate_id
            ) as gp_candidate_id,
            coalesce(
                gp_api.gp_election_id,
                br.gp_election_id,
                ts.gp_election_id,
                ddhq.gp_election_id
            ) as gp_election_id,
            -- gp_api-only columns
            gp_api.product_campaign_id,
            -- gp_api wins, then BR > TS > DDHQ (where applicable)
            {% for col in gp_api_wins_cols %}
                {% if col in ddhq_fallback_cols %}
                    coalesce(
                        gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}
                    ) as {{ col }},
                {% else %}
                    coalesce(gp_api.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
                {% endif %}
            {% endfor %}
            -- hubspot_company_ids: BR-only (gp_api / TS / DDHQ never set it).
            br.hubspot_company_ids,
            -- candidacy_result: DDHQ remains authoritative for results.
            coalesce(
                ddhq.candidacy_result,
                br.candidacy_result,
                ts.candidacy_result,
                gp_api.candidacy_result
            ) as candidacy_result,
            -- is_open_seat: BR > TS > DDHQ; gp_api carries no value.
            coalesce(
                br.is_open_seat, ts.is_open_seat, ddhq.is_open_seat
            ) as is_open_seat,
            -- gp_api wins (only source) for these PD-native flags.
            gp_api.is_pledged,
            gp_api.is_verified,
            gp_api.verification_status_reason,
            -- TS wins for is_incumbent (TS: 51k populated, BR: 0); gp_api/DDHQ
            -- excluded.
            coalesce(ts.is_incumbent, br.is_incumbent) as is_incumbent,
            -- office_type: BR > gp_api > DDHQ. BR derives office_type from
            -- BallotReady's normalized position name (low Other rate); gp_api
            -- derives it from raw onboarding free-text (high Other rate), so
            -- prefer BR when a matched BR row exists. TS carries none at this grain.
            -- DATA-1972: PR2 supersedes this for positioned rows via the
            -- int__civics_position_office_type crosswalk; this ordering remains
            -- the fallback when br_position_database_id is null.
            coalesce(
                br.office_type, gp_api.office_type, ddhq.office_type
            ) as office_type,
            -- br_position_database_id: gp_api > BR > TS. DDHQ doesn't carry it.
            coalesce(
                gp_api.br_position_database_id,
                br.br_position_database_id,
                ts.br_position_database_id
            ) as br_position_database_id,
            -- BR doesn't compute viability_score (always NULL upstream); TS's
            -- MLflow score fills it in via int__civics_candidacy_techspeed.
            -- win_number / win_number_model remain BR-only.
            coalesce(br.viability_score, ts.viability_score) as viability_score,
            br.win_number,
            br.win_number_model,
            ts.score_viability_automated,
            array_compact(
                array(
                    case when br.gp_candidacy_id is not null then 'ballotready' end,
                    case when ts.gp_candidacy_id is not null then 'techspeed' end,
                    case when ddhq.gp_candidacy_id is not null then 'ddhq' end,
                    case when gp_api.gp_candidacy_id is not null then 'gp_api' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidacy_techspeed") }} as ts
            on br.gp_candidacy_id = ts.gp_candidacy_id
        full outer join
            {{ ref("int__civics_candidacy_ddhq") }} as ddhq
            on coalesce(br.gp_candidacy_id, ts.gp_candidacy_id) = ddhq.gp_candidacy_id
        full outer join
            {{ ref("int__civics_candidacy_gp_api") }} as gp_api
            on coalesce(br.gp_candidacy_id, ts.gp_candidacy_id, ddhq.gp_candidacy_id)
            = gp_api.gp_candidacy_id
    ),

    combined as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            score_viability_automated,
            source_systems
        from archive_2025
        union all
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at,
            is_incumbent,
            office_type,
            br_position_database_id,
            score_viability_automated,
            source_systems
        from merged_since_2026
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    ),

    -- Restricts candidacy_stage rows to those whose election/position context
    -- matches the deduped candidacy. Upstream gp_candidacy_id collisions can
    -- attach stage rows from a different race to a candidacy, which would
    -- otherwise let us pick a stage outcome that doesn't belong to this
    -- mart row. The equality predicates are NULL-tolerant: when either side
    -- lacks context (common for 2025 archive rows), the stage row is kept.
    candidacy_stages_in_context as (
        select
            cs.gp_candidacy_id,
            cs.election_stage,
            cs.election_result,
            cs.updated_at,
            {{ election_stage_funnel_rank("cs.election_stage") }} as stage_rank
        from {{ ref("candidacy_stage") }} as cs
        left join
            {{ ref("election_stage") }} as es
            on cs.gp_election_stage_id = es.gp_election_stage_id
        inner join
            deduplicated as d
            on cs.gp_candidacy_id = d.gp_candidacy_id
            and (
                es.gp_election_id is null
                or d.gp_election_id is null
                or es.gp_election_id = d.gp_election_id
            )
            and (
                es.br_position_id is null
                or d.br_position_database_id is null
                or es.br_position_id = d.br_position_database_id
            )
        where cs.election_stage is not null
    ),

    -- Deepest stage the candidacy has REACHED. Unlike the decided rollup below,
    -- this keeps stages that have a candidacy_stage row but no result yet (an
    -- upcoming general, or a runoff that has not been called). Ordering: deepest
    -- stage first; within a stage prefer a row that already carries a result;
    -- updated_at breaks remaining ties (a known candidacy_stage data-quality
    -- issue tracked separately). This is what `latest_stage_reached` /
    -- `latest_stage_result` expose, so the pair can surface a still-undecided
    -- stage (result NULL) that the candidacy has advanced to.
    latest_stage_per_candidacy as (
        select
            gp_candidacy_id,
            election_stage as latest_stage_reached,
            election_result as latest_stage_result
        from candidacy_stages_in_context
        qualify
            row_number() over (
                partition by gp_candidacy_id
                order by
                    stage_rank desc,
                    case when election_result is not null then 1 else 0 end desc,
                    updated_at desc nulls last
            )
            = 1
    ),

    -- Deepest stage that has actually been DECIDED (non-null result). Used to
    -- keep candidacy_result populated with the last known outcome when the
    -- candidacy has advanced to a still-undecided deeper stage (e.g. won the
    -- primary while the general is pending -> "Won Primary").
    last_decided_stage_per_candidacy as (
        select
            gp_candidacy_id,
            election_stage as decided_stage,
            election_result as decided_result,
            stage_rank as decided_stage_rank
        from candidacy_stages_in_context
        where election_result is not null
        qualify
            row_number() over (
                partition by gp_candidacy_id
                order by stage_rank desc, updated_at desc nulls last
            )
            = 1
    ),

    -- Deepest stage that EXISTS for the whole race (across all candidacies),
    -- from election_stage. This tells us whether a decided primary is actually
    -- the deciding contest: when the race has no general (e.g. unopposed or a
    -- single-stage local race), the deepest race stage is the primary itself, so
    -- a primary win is a seat win rather than merely advancing. NOTE: this keys
    -- off the stages BallotReady has loaded for the race, so it does not capture
    -- jurisdictions where a majority at the primary wins the seat outright while
    -- a general is still scheduled (DATA-2042 follow-up).
    election_final_stage as (
        select
            gp_election_id,
            max({{ election_stage_funnel_rank("stage_type") }}) as race_max_stage_rank
        from {{ ref("election_stage") }}
        -- Exclude result-less stage placeholders: a NULL stage_type ranks 0 via
        -- the macro's else arm, which would make race_max_stage_rank 0 for an
        -- all-placeholder race and spuriously promote any decided stage to a bare
        -- seat result. Mirrors the `election_stage is not null` guard in
        -- candidacy_stages_in_context.
        where gp_election_id is not null and stage_type is not null
        group by gp_election_id
    ),

    final_candidacy as (
        select
            deduplicated.gp_candidacy_id,
            deduplicated.gp_candidate_id,
            deduplicated.gp_election_id,
            deduplicated.product_campaign_id,
            deduplicated.hubspot_contact_id,
            deduplicated.hubspot_company_ids,
            deduplicated.candidate_id_source,
            deduplicated.party_affiliation,
            deduplicated.is_incumbent,
            deduplicated.is_open_seat,
            deduplicated.candidate_office,
            deduplicated.official_office_name,
            deduplicated.office_level,
            -- DATA-1972: positioned rows inherit the canonical office_type from the
            -- position crosswalk whenever it classifies the position (non-Other).
            -- A per-source value survives when the crosswalk can only say 'Other',
            -- so a clean source value is never downgraded; rows without a position
            -- (e.g. 2025 HubSpot archive, TS-without-position) keep the per-source
            -- value; remaining blanks fill with the crosswalk's 'Other'.
            coalesce(
                nullif(pos_ot.office_type, 'Other'),
                deduplicated.office_type,
                pos_ot.office_type
            ) as office_type,
            -- Stage-aware overall outcome. A result decided at the race's FINAL stage
            -- is
            -- a seat outcome and stays bare ('Won' = won the seat / will serve). A
            -- result
            -- decided at an earlier stage is suffixed (" Primary" / " Primary
            -- Runoff") so
            -- it is never mistaken for a seat win, and a candidacy that has advanced
            -- to a
            -- still-uncalled runoff reads as 'Runoff' rather than a stale Won/Lost from
            -- the prior stage. A decided primary IS the seat outcome when the race
            -- has no
            -- deeper stage (decisive / single-stage race), so it stays bare too. Falls
            -- back to the provider-rolled result only when there is no in-context stage
            -- row.
            case
                when latest.latest_stage_reached is null
                then deduplicated.candidacy_result
                -- Advanced to an uncalled runoff: report the runoff rather than a
                -- stale prior-stage result. A pending primary runoff keeps its
                -- cycle suffix ('Runoff Primary'), matching the decided primary
                -- path; a pending general runoff reads bare 'Runoff'.
                when
                    latest.latest_stage_result is null
                    and latest.latest_stage_reached
                    in ('primary runoff', 'primary special runoff')
                then 'Runoff Primary'
                when
                    latest.latest_stage_result is null
                    and latest.latest_stage_reached
                    in ('general runoff', 'general special runoff')
                then 'Runoff'
                -- Decided at (or beyond) the race's deepest known stage: seat outcome.
                -- race_max_stage_rank is NULL when the race structure is unknown
                -- (NULL gp_election_id, no election_final_stage match); the
                -- comparison is then UNKNOWN and control falls through. That is
                -- intentional: with no race structure we will not promote an
                -- unconfirmed primary win to a bare seat 'Won'; it stays
                -- 'Won Primary' (is_elected NULL) via the suffix branch below.
                when decided.decided_stage_rank >= race.race_max_stage_rank
                then decided.decided_result
                -- Decided earlier than the race's final stage: not yet a seat outcome.
                when
                    decided.decided_result in ('Won', 'Lost', 'Runoff')
                    and decided.decided_stage in ('primary', 'primary special')
                then decided.decided_result || ' Primary'
                when
                    decided.decided_result in ('Won', 'Lost')
                    and decided.decided_stage
                    in ('primary runoff', 'primary special runoff')
                then decided.decided_result || ' Primary Runoff'
                else decided.decided_result
            end as candidacy_result,
            case
                when
                    latest.latest_stage_reached in (
                        'general',
                        'general runoff',
                        'general special',
                        'general special runoff'
                    )
                then latest.latest_stage_result
            end as general_election_result,
            latest.latest_stage_reached,
            latest.latest_stage_result,
            deduplicated.is_pledged,
            deduplicated.is_verified,
            deduplicated.verification_status_reason,
            deduplicated.is_partisan,
            deduplicated.primary_election_date,
            deduplicated.primary_runoff_election_date,
            deduplicated.general_election_date,
            deduplicated.general_runoff_election_date,
            deduplicated.br_position_database_id,
            deduplicated.viability_score,
            deduplicated.win_number,
            deduplicated.win_number_model,
            {{
                win_icp_date_gate(
                    icp_attribute="icp.icp_office_win",
                    primary_date="deduplicated.primary_election_date",
                    primary_runoff_date="deduplicated.primary_runoff_election_date",
                    general_date="deduplicated.general_election_date",
                    general_runoff_date="deduplicated.general_runoff_election_date",
                    effective_date="icp.icp_win_effective_date",
                )
            }} as is_win_icp,
            icp.icp_office_serve as is_serve_icp,
            {{
                win_icp_date_gate(
                    icp_attribute="icp.icp_win_supersize",
                    primary_date="deduplicated.primary_election_date",
                    primary_runoff_date="deduplicated.primary_runoff_election_date",
                    general_date="deduplicated.general_election_date",
                    general_runoff_date="deduplicated.general_runoff_election_date",
                    effective_date="icp.icp_win_effective_date",
                )
            }} as is_win_supersize_icp,
            deduplicated.score_viability_automated,
            deduplicated.source_systems,
            deduplicated.created_at,
            deduplicated.updated_at

        from deduplicated
        left join
            {{ ref("int__icp_offices") }} as icp
            on deduplicated.br_position_database_id = icp.br_database_position_id
        left join
            {{ ref("int__civics_position_office_type") }} as pos_ot
            on deduplicated.br_position_database_id = pos_ot.br_position_database_id
        left join
            latest_stage_per_candidacy as latest
            on deduplicated.gp_candidacy_id = latest.gp_candidacy_id
        left join
            last_decided_stage_per_candidacy as decided
            on deduplicated.gp_candidacy_id = decided.gp_candidacy_id
        left join
            election_final_stage as race
            on deduplicated.gp_election_id = race.gp_election_id
    )

select
    *,
    -- Did the candidacy win the seat (and will therefore serve)? TRUE only on a
    -- confirmed win at the race's final stage; FALSE on a known non-win; NULL
    -- when the outcome is not known to be a win or a loss — still undecided
    -- (advanced past a primary, in a runoff, no result captured yet) OR
    -- indeterminate ('Cannot Determine'). NULL deliberately is not "lost": we
    -- only assert FALSE when a candidate is known not to have won the seat, so a
    -- 'Cannot Determine' is not silently counted as a loss. Derived from the
    -- final candidacy_result, which is unambiguous here because final_candidacy
    -- exposes no rival column.
    case
        when candidacy_result = 'Won'
        then true
        when
            candidacy_result in (
                'Lost',
                'Lost Primary',
                'Lost Primary Runoff',
                'Withdrew',
                'Not on Ballot'
            )
        then false
    end as is_elected
from final_candidacy
