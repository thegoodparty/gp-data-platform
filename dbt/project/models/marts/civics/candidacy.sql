-- Civics mart candidacy table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
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
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            general_election_date,
            runoff_election_date,
            br_position_database_id,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}

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
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            general_election_date,
            runoff_election_date,
            br_position_database_id,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at
        from {{ ref("int__civics_candidacy_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    gp_candidacy_id,
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
    office_type,
    candidacy_result,
    is_pledged,
    is_verified,
    verification_status_reason,
    is_partisan,
    primary_election_date,
    general_election_date,
    runoff_election_date,
    br_position_database_id,
    viability_score,
    win_number,
    win_number_model,
    created_at,
    updated_at

from deduplicated
