{{
    config(
        materialized="table",
        tags=["intermediate", "civics", "election", "archive"],
    )
}}

-- Historical archive of elections from elections on or before 2025-12-31
-- Uses archived HubSpot data from 2026-01-22 snapshot
-- Inlines m_general__election logic for self-contained archive
with
    elections as (
        select
            -- Identifiers
            {{ generate_gp_election_id("tbl_contest") }} as gp_election_id,
            tbl_contest.official_office_name,
            tbl_contest.candidate_office,
            tbl_contest.office_level,
            tbl_contest.office_type,
            tbl_contest.state,
            tbl_contest.city,
            tbl_contest.district,
            tbl_contest.seat_name,
            tbl_contest.election_date,
            tbl_contest.election_year,
            tbl_contest.filing_deadline,
            tbl_contest.population,
            tbl_contest.seats_available,
            tbl_contest.term_start_date,
            tbl_contest.uncontested as is_uncontested,
            tbl_contest.number_of_opponents,
            tbl_contest.open_seat,
            case
                when tbl_ddhq_matches.ddhq_race_id is not null then true else false
            end as has_ddhq_match,
            tbl_contest.created_at,
            tbl_contest.updated_at
        from {{ ref("int__hubspot_contest_2025") }} as tbl_contest
        left join
            {{ ref("candidacy_2025") }} as tbl_candidacy
            on tbl_candidacy.hubspot_contact_id = tbl_contest.contact_id
        left join
            {{ ref("stg_model_predictions__candidacy_ddhq_matches_20251016") }}
            as tbl_ddhq_matches
            on tbl_ddhq_matches.gp_candidacy_id = tbl_candidacy.gp_candidacy_id
    ),

    -- Filter to elections on or before 2025-12-31
    archived_elections as (
        select *
        from elections
        where election_date <= '2025-12-31' and election_date >= '1900-01-01'
        qualify
            row_number() over (
                partition by gp_election_id
                order by has_ddhq_match desc, updated_at desc
            )
            = 1
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
