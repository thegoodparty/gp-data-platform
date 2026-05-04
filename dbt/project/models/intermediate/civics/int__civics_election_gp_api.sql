{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- gp_api participation indicator at election grain. gp_api doesn't hold
-- unique election data — every gp_api candidacy is anchored to a BR
-- position via ballotready_position_id, and BR remains the authoritative
-- source for descriptive election data. This model emits one row per
-- gp_election_id that gp_api participates in. Descriptive columns are
-- NULL by design; the mart's FOJ coalesce picks BR's values.
--
-- br_position_database_id is populated only when the gp_election_id
-- matches BR's election spine (BR-anchored row). For non-BR cluster
-- canonical_gp_election_ids, it stays NULL — otherwise downstream joins
-- on br_position_database_id (e.g., m_election_api__zip_to_position) would
-- fan out across the BR-natural row + the cluster-derived row that share
-- the same underlying campaign's ballotready_position_id.
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
            gp_election_id, min(created_at) as created_at, max(updated_at) as updated_at
        from {{ ref("int__civics_candidacy_gp_api") }}
        where gp_election_id is not null
        group by gp_election_id
    ),

    -- BR FK lookup. Only populates br_position_database_id when the
    -- gp_election_id is in BR's spine; cluster-derived ids return NULL FKs.
    br_lookup as (
        select gp_election_id, any_value(br_position_id) as br_position_database_id
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id
    )

select
    g.gp_election_id,
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
    br.br_position_database_id,
    cast(null as boolean) as is_judicial,
    cast(null as boolean) as is_appointed,
    cast(null as string) as br_normalized_position_type,
    g.created_at,
    g.updated_at
from gp_api_elections as g
left join br_lookup as br on g.gp_election_id = br.gp_election_id
