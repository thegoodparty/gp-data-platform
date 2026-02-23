-- Civics mart election table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
select
    e.gp_election_id,
    e.official_office_name,
    e.candidate_office,
    e.office_level,
    e.office_type,
    e.state,
    e.city,
    e.district,
    e.seat_name,
    e.election_date,
    e.election_year,
    e.filing_deadline,
    e.population,
    e.seats_available,
    e.term_start_date,
    e.is_uncontested,
    e.number_of_opponents,
    e.open_seat,
    e.has_ddhq_match,
    e.br_position_database_id,
    e.is_judicial,
    e.is_appointed,
    e.br_normalized_position_type,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    e.created_at,
    e.updated_at

from {{ ref("int__civics_election_2025") }} as e
left join
    {{ ref("int__icp_offices") }} as icp
    on e.br_position_database_id = icp.br_database_position_id
