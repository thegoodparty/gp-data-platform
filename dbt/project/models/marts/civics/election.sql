{{
    config(
        materialized="table",
        tags=["mart", "civics"],
    )
}}

with
    archived_elections as (
        -- Historical archive: elections on or before 2025-12-31
        -- Filter out invalid dates (e.g., years like 0028, 1024 which are data entry
        -- errors)
        select *
        from {{ ref("m_general__election") }}
        where election_date <= '2025-12-31' and election_date >= '1900-01-01'
    )

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
    created_at,
    updated_at

from archived_elections
