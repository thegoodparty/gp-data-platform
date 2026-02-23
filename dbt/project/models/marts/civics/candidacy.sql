-- Civics mart candidacy table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
select
    c.gp_candidacy_id,
    c.candidacy_id,
    c.gp_candidate_id,
    c.gp_election_id,
    c.product_campaign_id,
    c.hubspot_contact_id,
    c.hubspot_company_ids,
    c.candidate_id_source,
    c.party_affiliation,
    c.is_incumbent,
    c.is_open_seat,
    c.candidate_office,
    c.official_office_name,
    c.office_level,
    c.office_type,
    c.candidacy_result,
    c.is_pledged,
    c.is_verified,
    c.verification_status_reason,
    c.is_partisan,
    c.primary_election_date,
    c.general_election_date,
    c.runoff_election_date,
    c.br_position_database_id,
    c.viability_score,
    c.win_number,
    c.win_number_model,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    c.created_at,
    c.updated_at

from {{ ref("int__civics_candidacy_2025") }} as c
left join
    {{ ref("int__icp_offices") }} as icp
    on c.br_position_database_id = icp.br_database_position_id
