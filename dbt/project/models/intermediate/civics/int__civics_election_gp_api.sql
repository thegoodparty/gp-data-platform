{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- gp_api participation indicator at election grain. gp_api doesn't hold
-- unique election data — every gp_api candidacy is anchored to a BR
-- position via ballotready_position_id, and BR remains the authoritative
-- source for descriptive election data. This model emits one row per
-- gp_election_id that gp_api participates in. Descriptive columns are
-- NULL by design; the mart's FOJ coalesce picks BR's values.
--
-- gp_api candidacies prefer BR's natural gp_election_id over canonical_ids
-- cluster-derived values (see int__civics_candidacy_gp_api), so every row
-- here has a BR-natural gp_election_id and the FOJ in election.sql merges
-- it cleanly into the corresponding BR row.
--
-- Schema mirrors int__civics_election_techspeed for FOJ alignment in
-- the election mart.
with
    gp_api_elections as (
        -- created_at / updated_at use min/max of contributing candidacy
        -- timestamps so gp_api-only rows have non-null audit timestamps
        -- at the mart layer (the mart's coalesce still prefers BR/TS/DDHQ
        -- when present).
        select
            gp_election_id,
            max(br_position_database_id) as br_position_database_id,
            min(created_at) as created_at,
            max(updated_at) as updated_at
        from {{ ref("int__civics_candidacy_gp_api") }}
        where gp_election_id is not null
        group by gp_election_id
    )

select
    gp_election_id,
    cast(null as string) as official_office_name,
    cast(null as string) as candidate_office,
    cast(null as string) as office_level,
    cast(null as string) as office_type,
    cast(null as string) as state,
    cast(null as string) as city,
    cast(null as string) as district,
    cast(null as string) as seat_name,
    cast(null as date) as election_date,
    cast(null as int) as election_year,
    cast(null as date) as filing_deadline,
    cast(null as int) as population,
    cast(null as int) as seats_available,
    cast(null as date) as term_start_date,
    cast(null as boolean) as is_uncontested,
    cast(null as string) as number_of_opponents,
    cast(null as boolean) as is_open_seat,
    false as has_ddhq_match,
    br_position_database_id,
    cast(null as boolean) as is_judicial,
    cast(null as boolean) as is_appointed,
    cast(null as string) as br_normalized_position_type,
    created_at,
    updated_at
from gp_api_elections
