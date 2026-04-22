{{ config(tags=["archive"]) }}

-- Historical archive of candidacy stages from elections on or before 2025-12-31.
-- Grain: one row per (candidacy, stage_type) where the candidacy carries a
-- non-null date for that stage. DDHQ is an optional attribute join; HubSpot-
-- only candidacies (no DDHQ match) now flow through.
with
    candidacies as (
        select
            gp_candidacy_id,
            gp_election_id,
            br_position_database_id,
            candidacy_result,
            primary_election_date,
            general_election_date,
            general_runoff_election_date,
            created_at,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Unpivot candidacy dates into candidacy-stage rows.
    -- candidacy_result on the candidacy is general-stage-oriented (HubSpot
    -- only records general-stage results), so only attach it to general rows.
    candidacy_stages_seed as (
        select
            gp_candidacy_id,
            gp_election_id,
            br_position_database_id,
            'primary' as stage_type,
            primary_election_date as election_stage_date,
            cast(null as string) as hubspot_stage_result,
            created_at,
            updated_at
        from candidacies
        where primary_election_date between '1900-01-01' and '2025-12-31'

        union all

        select
            gp_candidacy_id,
            gp_election_id,
            br_position_database_id,
            'general' as stage_type,
            general_election_date as election_stage_date,
            candidacy_result as hubspot_stage_result,
            created_at,
            updated_at
        from candidacies
        where general_election_date between '1900-01-01' and '2025-12-31'

        union all

        select
            gp_candidacy_id,
            gp_election_id,
            br_position_database_id,
            'general runoff' as stage_type,
            general_runoff_election_date as election_stage_date,
            cast(null as string) as hubspot_stage_result,
            created_at,
            updated_at
        from candidacies
        where general_runoff_election_date between '1900-01-01' and '2025-12-31'
    ),

    -- Pre-project normalized stage_type so downstream joins can stay flat.
    ddhq_matches as (
        select
            gp_candidacy_id,
            ddhq_race_id,
            ddhq_candidate_id,
            ddhq_candidate,
            ddhq_candidate_party,
            ddhq_is_winner,
            llm_confidence,
            llm_reasoning,
            top_10_candidates,
            has_match,
            {{ normalize_ddhq_stage_type("ddhq_election_type") }} as stage_type
        from {{ ref("int__gp_ai_election_match") }}
    ),

    -- Attach DDHQ match (optional).
    with_ddhq as (
        select
            s.gp_candidacy_id,
            s.gp_election_id,
            s.br_position_database_id,
            s.stage_type,
            s.election_stage_date,
            s.hubspot_stage_result,
            s.created_at,
            s.updated_at,
            m.ddhq_race_id,
            m.ddhq_candidate_id,
            m.ddhq_candidate,
            m.ddhq_candidate_party,
            m.ddhq_is_winner,
            m.llm_confidence,
            m.llm_reasoning,
            m.top_10_candidates,
            m.has_match,
            r.votes as votes_received
        from candidacy_stages_seed as s
        left join
            ddhq_matches as m
            on s.gp_candidacy_id = m.gp_candidacy_id
            and s.stage_type = m.stage_type
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }} as r
            on r.ddhq_race_id = m.ddhq_race_id
            and r.candidate_id = m.ddhq_candidate_id
        qualify
            row_number() over (
                partition by s.gp_candidacy_id, s.stage_type
                order by m.has_match desc nulls last, s.updated_at desc
            )
            = 1
    ),

    -- Compute candidate gp_election_stage_id using the same conditional
    -- salting as int__civics_election_stage_2025: DDHQ-matched candidacies
    -- salt on ddhq_race_id (preserves pre-PR IDs and distinguishes distinct
    -- DDHQ races); HubSpot-only candidacies salt on (gp_election_id, stage_type).
    with_candidate_stage_id as (
        select
            w.*,
            case
                when w.ddhq_race_id is not null
                then {{ generate_salted_uuid(fields=["w.ddhq_race_id"]) }}
                else
                    {{
                        generate_salted_uuid(
                            fields=["w.gp_election_id", "w.stage_type"]
                        )
                    }}
            end as candidate_gp_election_stage_id
        from with_ddhq as w
    ),

    -- FK gate: only keep candidate_gp_election_stage_id when it exists in
    -- election_stage_2025. HubSpot-only candidacies whose (gp_election_id,
    -- stage_type) is already covered by a DDHQ race (so the HubSpot-only path
    -- didn't materialize) land with gp_election_stage_id = null.
    valid_stage_ids as (
        select gp_election_stage_id from {{ ref("int__civics_election_stage_2025") }}
    )

select
    {{ generate_salted_uuid(fields=["w.gp_candidacy_id", "w.stage_type"]) }}
    as gp_candidacy_stage_id,
    w.gp_candidacy_id,
    case
        when v.gp_election_stage_id is not null
        then w.candidate_gp_election_stage_id
        else null
    end as gp_election_stage_id,
    w.ddhq_candidate as candidate_name,
    w.ddhq_candidate_id as source_candidate_id,
    w.ddhq_race_id as source_race_id,
    w.ddhq_candidate_party as candidate_party,
    -- Derive is_winner from DDHQ first, then fall back to the HubSpot-sourced
    -- stage result so pledged HubSpot Wins aren't left with is_winner = null.
    case
        when w.ddhq_is_winner = 'Y'
        then true
        when w.ddhq_is_winner = 'N'
        then false
        when w.hubspot_stage_result = 'Won'
        then true
        when w.hubspot_stage_result = 'Lost'
        then false
        else null
    end as is_winner,
    -- Stage-aware result: HubSpot carries general-stage results only.
    -- "Cannot Determine" is a valid candidacy_result but not an accepted
    -- stage-level election_result value, so normalize it to null.
    case
        when
            w.stage_type = 'general'
            and w.hubspot_stage_result is not null
            and w.hubspot_stage_result != 'Cannot Determine'
        then w.hubspot_stage_result
        when w.ddhq_is_winner = 'Y'
        then 'Won'
        when w.ddhq_is_winner = 'N'
        then 'Lost'
        else null
    end as election_result,
    case
        when
            w.stage_type = 'general'
            and w.hubspot_stage_result is not null
            and w.hubspot_stage_result != 'Cannot Determine'
        then 'hubspot'
        when w.ddhq_is_winner is not null
        then 'ddhq'
        else null
    end as election_result_source,
    w.llm_confidence as match_confidence,
    w.llm_reasoning as match_reasoning,
    w.top_10_candidates as match_top_candidates,
    w.has_match,
    w.votes_received,
    w.election_stage_date,
    w.created_at,
    w.updated_at
from with_candidate_stage_id as w
left join
    valid_stage_ids as v on w.candidate_gp_election_stage_id = v.gp_election_stage_id
