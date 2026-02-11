-- Civics mart election table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
select
    gp_election_id,
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    state,
    city,
    district,
    seat_name,
    election_date,
    election_year,
    filing_deadline,
    population,
    seats_available,
    term_start_date,
    is_uncontested,
    number_of_opponents,
    open_seat,
    has_ddhq_match,
    br_position_database_id,
    is_judicial,
    is_appointed,
    br_normalized_position_type,
    created_at,
    updated_at

from {{ ref("int__civics_election_2025") }}
