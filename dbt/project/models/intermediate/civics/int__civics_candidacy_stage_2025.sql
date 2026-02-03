{{ config(tags=["archive"]) }}

-- Historical archive of candidacy stages from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Uses companies-based model for better coverage (joins via companies.contacts field)
with
    candidacy_stages as (
        select
            -- Identifiers
            {{
                generate_salted_uuid(
                    fields=[
                        "tbl_companies.gp_candidacy_id",
                        "tbl_ddhq_matches.ddhq_race_id",
                    ]
                )
            }} as gp_candidacy_stage_id,
            tbl_companies.gp_candidacy_id,
            case
                when tbl_ddhq_matches.ddhq_race_id is not null
                then
                    {{ generate_salted_uuid(fields=["tbl_ddhq_matches.ddhq_race_id"]) }}
                else null
            end as gp_election_stage_id,
            tbl_ddhq_matches.ddhq_candidate as candidate_name,
            tbl_ddhq_matches.ddhq_candidate_id as source_candidate_id,
            tbl_ddhq_matches.ddhq_race_id as source_race_id,
            tbl_ddhq_matches.ddhq_candidate_party as candidate_party,
            case
                when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                then true
                when tbl_ddhq_matches.ddhq_is_winner = 'N'
                then false
                else null
            end as is_winner,
            -- Coalesce HubSpot companies result with DDHQ result for comprehensive
            -- coverage
            -- HubSpot only has general election results, so only use it for general
            -- stages
            coalesce(
                case
                    when lower(tbl_ddhq_matches.ddhq_election_type) = 'general'
                    then hs_companies.properties_general_election_result
                    else null
                end,
                case
                    when tbl_ddhq_matches.ddhq_is_winner = 'Y'
                    then 'Won'
                    when tbl_ddhq_matches.ddhq_is_winner = 'N'
                    then 'Lost'
                    else null
                end
            ) as election_result,
            -- Source of the election result
            case
                when
                    lower(tbl_ddhq_matches.ddhq_election_type) = 'general'
                    and hs_companies.properties_general_election_result is not null
                then 'hubspot'
                when tbl_ddhq_matches.ddhq_is_winner is not null
                then 'ddhq'
                else null
            end as election_result_source,
            tbl_ddhq_matches.llm_confidence as match_confidence,
            tbl_ddhq_matches.llm_reasoning as match_reasoning,
            tbl_ddhq_matches.top_10_candidates as match_top_candidates,
            tbl_ddhq_matches.has_match,
            tbl_ddhq_election_results_source.votes as votes_received,
            tbl_ddhq_election_results_source.date as election_stage_date,
            tbl_companies.created_at,
            tbl_companies.updated_at
        from {{ ref("int__hubspot_companies_w_contacts_2025") }} as tbl_companies
        left join
            {{ ref("int__gp_ai_election_match") }} as tbl_ddhq_matches
            on tbl_companies.gp_candidacy_id = tbl_ddhq_matches.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }}
            as tbl_ddhq_election_results_source
            on tbl_ddhq_election_results_source.race_id = tbl_ddhq_matches.ddhq_race_id
            and tbl_ddhq_election_results_source.candidate_id
            = tbl_ddhq_matches.ddhq_candidate_id
        left join
            {{ ref("int__hubspot_companies_archive_2025") }} as hs_companies
            on tbl_companies.company_id = hs_companies.id
        qualify
            row_number() over (
                partition by gp_candidacy_stage_id order by updated_at desc
            )
            = 1
    ),

    -- Filter to election stages on or before 2025-12-31
    archived_candidacy_stages as (
        select *
        from candidacy_stages
        where
            election_stage_date <= '2025-12-31' and election_stage_date >= '1900-01-01'
    ),

    -- Only include candidacy_stages that have a matching candidacy in the archive
    valid_candidacies as (
        select gp_candidacy_id from {{ ref("int__civics_candidacy_2025") }}
    ),

    -- Only include candidacy_stages that have a matching election_stage in the archive
    valid_election_stages as (
        select gp_election_stage_id from {{ ref("int__civics_election_stage_2025") }}
    ),

    -- Filter to valid records
    filtered_candidacy_stages as (
        select stage.*
        from archived_candidacy_stages as stage
        inner join
            valid_candidacies as candidacy
            on stage.gp_candidacy_id = candidacy.gp_candidacy_id
        where
            stage.gp_election_stage_id is null
            or stage.gp_election_stage_id
            in (select gp_election_stage_id from valid_election_stages)
    )

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
    election_stage_date,
    created_at,
    updated_at

from filtered_candidacy_stages
