{{
    config(
        materialized="table",
    )
}}

with
    archived_candidacies as (
        -- Historical archive: elections on or before 2025-12-31
        -- Filter out invalid dates (e.g., years like 0028, 1024 which are data entry
        -- errors)
        select *
        from {{ ref("m_general__candidacy_v2") }}
        where
            general_election_date <= '2025-12-31'
            and general_election_date >= '1900-01-01'
    ),

    -- Only include candidacies that have a matching election in the archive
    valid_elections as (select gp_election_id from {{ ref("election_20260122") }}),

    filtered_candidacies as (
        select *
        from archived_candidacies
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
