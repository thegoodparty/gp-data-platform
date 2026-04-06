-- DDHQ → Civics mart election
-- Derived from: int__civics_candidacy_stage_ddhq
--
-- Grain: One row per election (position + election year)
--
-- Uses a representative-row approach: picks one candidacy_stage row per
-- gp_election_id, preferring general stage and the latest timestamp.
with
    source as (select * from {{ ref("int__civics_candidacy_stage_ddhq") }}),

    representative_row as (
        select *
        from source
        qualify
            row_number() over (
                partition by gp_election_id
                order by
                    case when election_stage = 'general' then 0 else 1 end,
                    updated_at desc,
                    candidate_full_name
            )
            = 1
    )

select
    gp_election_id,
    official_office_name,
    candidate_office,
    office_level,
    office_type,
    state_name as state,
    state_postal_code,
    cast(null as string) as city,
    district,
    cast(null as string) as seat_name,
    election_date,
    year(election_date) as election_year,
    cast(null as date) as filing_deadline,
    cast(null as int) as population,
    number_of_seats_in_election as seats_available,
    cast(null as date) as term_start_date,
    is_uncontested,
    cast(null as string) as number_of_opponents,
    cast(null as boolean) as is_open_seat,
    true as has_ddhq_match,
    cast(null as bigint) as br_position_database_id,
    cast(null as boolean) as is_judicial,
    cast(null as boolean) as is_appointed,
    cast(null as string) as br_normalized_position_type,
    created_at,
    updated_at
from representative_row
