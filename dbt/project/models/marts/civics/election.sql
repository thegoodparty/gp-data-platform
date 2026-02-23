-- Civics mart election table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
    combined as (
        select *
        from {{ ref("int__civics_election_2025") }}
        union all
        select *
        from {{ ref("int__civics_election_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_election_id order by updated_at desc) = 1
    )

select
    deduplicated.gp_election_id,
    deduplicated.official_office_name,
    deduplicated.candidate_office,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.state,
    deduplicated.city,
    deduplicated.district,
    deduplicated.seat_name,
    deduplicated.election_date,
    deduplicated.election_year,
    deduplicated.filing_deadline,
    deduplicated.population,
    deduplicated.seats_available,
    deduplicated.term_start_date,
    deduplicated.is_uncontested,
    deduplicated.number_of_opponents,
    deduplicated.open_seat,
    deduplicated.has_ddhq_match,
    deduplicated.br_position_database_id,
    deduplicated.is_judicial,
    deduplicated.is_appointed,
    deduplicated.br_normalized_position_type,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
