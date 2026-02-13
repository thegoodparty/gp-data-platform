-- Civics mart candidacy table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
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
