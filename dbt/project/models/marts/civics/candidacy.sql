{{
    config(
        materialized="table",
    )
}}

-- Civics mart candidacy table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
-- Filters to only include candidacies with valid elections for referential integrity
with
    -- Only include candidacies that have a matching election in the archive
    valid_elections as (select gp_election_id from {{ ref("election") }}),

    filtered_candidacies as (
        select *
        from {{ ref("candidacy_20260122") }}
        where
            gp_election_id is null
            or gp_election_id in (select gp_election_id from valid_elections)
    )

select
    gp_candidacy_id,
    candidacy_id,
    gp_candidate_id,
    gp_election_id,
    product_campaign_id,
    hubspot_contact_id,
    hubspot_company_ids,
    candidate_id_source,
    party_affiliation,
    is_incumbent,
    is_open_seat,
    candidate_office,
    official_office_name,
    office_level,
    candidacy_result,
    pledge_status,
    verified_candidate,
    is_partisan,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at

from filtered_candidacies
