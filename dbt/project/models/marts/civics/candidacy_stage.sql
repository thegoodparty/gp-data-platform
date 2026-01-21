{{
    config(
        materialized="table",
        tags=["mart", "civics", "historical"],
    )
}}

with
    -- Get one company_id per gp_candidacy_id to avoid duplicates
    candidacy_companies as (
        select gp_candidacy_id, company_id
        from {{ ref("int__hubspot_contacts_w_companies") }}
        where company_id is not null
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    ),

    archived_candidacy_stages as (
        -- Historical archive: election stages on or before 2025-12-31
        select stage.*, companies.company_id
        from {{ ref("m_general__candidacy_stage") }} as stage
        left join
            candidacy_companies as companies
            on stage.gp_candidacy_id = companies.gp_candidacy_id
        where stage.ddhq_election_stage_date <= '2025-12-31'
    ),

    -- Only include candidacy_stages that have a matching candidacy in the archive
    valid_candidacies as (select gp_candidacy_id from {{ ref("candidacy") }}),

    -- Only include candidacy_stages that have a matching election_stage in the archive
    valid_election_stages as (
        select gp_election_stage_id from {{ ref("election_stage") }}
    )

select
    stage.gp_candidacy_stage_id,
    stage.gp_candidacy_id,
    stage.gp_election_stage_id,
    stage.ddhq_candidate,
    stage.ddhq_candidate_id,
    stage.ddhq_race_id,
    stage.ddhq_candidate_party,
    stage.ddhq_is_winner,
    -- Coalesce HubSpot companies result with DDHQ result for comprehensive coverage
    -- HubSpot only has general election results, so only use it for general stages
    coalesce(
        case
            when lower(election_stage.election_stage) = 'general'
            then hs_companies.properties_general_election_result
            else null
        end,
        case
            when stage.ddhq_is_winner = true
            then 'Won'
            when stage.ddhq_is_winner = false
            then 'Lost'
            else null
        end
    ) as candidacy_stage_result,
    -- Source of the election result
    case
        when
            lower(election_stage.election_stage) = 'general'
            and hs_companies.properties_general_election_result is not null
        then 'hubspot'
        when stage.ddhq_is_winner is not null
        then 'ddhq'
        else null
    end as candidacy_stage_result_source,
    stage.ddhq_llm_confidence,
    stage.ddhq_llm_reasoning,
    stage.ddhq_top_10_candidates,
    stage.ddhq_has_match,
    stage.votes_received,
    stage.ddhq_election_stage_date,
    stage.created_at,
    stage.updated_at

from archived_candidacy_stages as stage
left join
    {{ ref("stg_airbyte_source__hubspot_api_companies") }} as hs_companies
    on stage.company_id = hs_companies.id
left join
    {{ ref("election_stage") }} as election_stage
    on stage.gp_election_stage_id = election_stage.gp_election_stage_id
inner join
    valid_candidacies as candidacy on stage.gp_candidacy_id = candidacy.gp_candidacy_id
-- Filter to only include records with valid election_stage references (or null)
where
    stage.gp_election_stage_id is null
    or stage.gp_election_stage_id
    in (select gp_election_stage_id from valid_election_stages)
