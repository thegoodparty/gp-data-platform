-- Civics mart candidacy_stage table
-- Union of 2025 HubSpot archive and 2026+ merged BallotReady + TechSpeed + DDHQ.
-- TS and DDHQ int models remap clustered IDs to BR canonicals via
-- int__civics_er_canonical_ids, so merging collapses to a full outer join on
-- shared gp_candidacy_stage_id.
--
-- Provider precedence:
-- DDHQ wins for election outcome columns (is_winner, election_result,
-- election_result_source, votes_received, is_uncontested) — DDHQ is the
-- authoritative source for past-race results.
-- BR > TS > DDHQ for descriptive columns (candidate_name, source_*,
-- candidate_party, election_stage_date, timestamps).
-- match_* and has_match come from BR/TS only (DDHQ has no LLM-match metadata).
{%- set br_wins_cols = [
    "candidate_name",
    "source_candidate_id",
    "source_race_id",
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
    archive_2025 as (
        select
            gp_candidacy_stage_id,
            gp_candidacy_id,
            gp_election_stage_id,
            -- Column order must match merged_since_2026 (br_wins, ddhq_wins,
            -- BR/TS-only match_*, is_uncontested, source_systems)
            candidate_name,
            cast(source_candidate_id as string) as source_candidate_id,
            cast(source_race_id as string) as source_race_id,
            candidate_party,
            election_stage_date,
            created_at,
            updated_at,
            is_winner,
            election_result,
            election_result_source,
            votes_received,
            cast(match_confidence as float) as match_confidence,
            match_reasoning,
            match_top_candidates,
            has_match,
            cast(null as boolean) as is_uncontested,
            array_compact(
                array('hubspot', case when has_match then 'ddhq' end)
            ) as source_systems
        from {{ ref("int__civics_candidacy_stage_2025") }}
    ),

    -- DDHQ projected into the BR/TS merge-row schema (column rename only).
    -- Done up front so the coalesce loops below stay symmetric across providers.
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
            created_at,
            updated_at,
            is_winner,
            election_result,
            election_result_source,
            cast(votes as string) as votes_received,
            is_uncontested
        from {{ ref("int__civics_candidacy_stage_ddhq") }}
    ),

    -- Three-way merge for 2026+ data. BR, TS, and DDHQ int models all expose
    -- shared canonical gp_* IDs via int__civics_er_canonical_ids when Splink
    -- clustered the row, so a full outer join on gp_candidacy_stage_id
    -- merges matched triples; unmatched rows pass through as new rows with
    -- NULLs on the absent providers (e.g. DDHQ-only races where Splink
    -- found no BR/TS counterpart).
    merged_since_2026 as (
        select
            coalesce(
                br.gp_candidacy_stage_id,
                ts.gp_candidacy_stage_id,
                ddhq.gp_candidacy_stage_id
            ) as gp_candidacy_stage_id,
            coalesce(
                br.gp_candidacy_id, ts.gp_candidacy_id, ddhq.gp_candidacy_id
            ) as gp_candidacy_id,
            coalesce(
                br.gp_election_stage_id,
                ts.gp_election_stage_id,
                ddhq.gp_election_stage_id
            ) as gp_election_stage_id,
            {% for col in br_wins_cols %}
                coalesce(br.{{ col }}, ts.{{ col }}, ddhq.{{ col }}) as {{ col }},
            {% endfor %}
            {% for col in ddhq_wins_cols %}
                coalesce(ddhq.{{ col }}, br.{{ col }}, ts.{{ col }}) as {{ col }},
            {% endfor %}
            -- LLM-match metadata only exists on BR/TS legacy paths.
            coalesce(br.match_confidence, ts.match_confidence) as match_confidence,
            coalesce(br.match_reasoning, ts.match_reasoning) as match_reasoning,
            coalesce(
                br.match_top_candidates, ts.match_top_candidates
            ) as match_top_candidates,
            coalesce(br.has_match, ts.has_match) as has_match,
            -- is_uncontested only exists on DDHQ at this grain.
            ddhq.is_uncontested,
            array_compact(
                array(
                    case
                        when br.gp_candidacy_stage_id is not null then 'ballotready'
                    end,
                    case when ts.gp_candidacy_stage_id is not null then 'techspeed' end,
                    case when ddhq.gp_candidacy_stage_id is not null then 'ddhq' end
                )
            ) as source_systems
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as br
        full outer join
            {{ ref("int__civics_candidacy_stage_techspeed") }} as ts
            on br.gp_candidacy_stage_id = ts.gp_candidacy_stage_id
        full outer join
            ddhq
            on coalesce(br.gp_candidacy_stage_id, ts.gp_candidacy_stage_id)
            = ddhq.gp_candidacy_stage_id
    ),

    combined as (
        select *
        from archive_2025
        union all
        select *
        from merged_since_2026
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
    es.is_win_icp,
    es.is_serve_icp,
    es.is_win_supersize_icp,
    deduplicated.source_systems,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("election_stage") }} as es
    on deduplicated.gp_election_stage_id = es.gp_election_stage_id
