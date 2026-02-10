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
    br_position.judicial as is_judicial,
    br_position.appointed as is_appointed,
    br_normalized.name as br_normalized_position_type,
    e.created_at,
    e.updated_at

from {{ ref("int__civics_election_2025") }} as e

left join
    {{ ref("stg_airbyte_source__ballotready_api_position") }} as br_position
    on e.br_position_database_id = br_position.database_id

left join
    {{ ref("int__ballotready_normalized_position") }} as br_normalized
    on br_position.normalized_position.`databaseId` = br_normalized.database_id
