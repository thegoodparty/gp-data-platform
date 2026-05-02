{{ config(materialized="table", tags=["civics", "gp_api"]) }}

-- gp_api participation indicator at election_stage grain. gp_api doesn't
-- hold unique election_stage data — every gp_api candidacy_stage links
-- back to BR via br_position_id and (transitively) br_race_id, and BR
-- remains the authoritative source for descriptive race data. This model
-- emits one row per gp_election_stage_id that gp_api participates in.
-- Descriptive columns (stage_type, election_date, race_name, etc.) are
-- NULL by design; the mart's FOJ coalesce picks BR's values.
--
-- Schema mirrors int__civics_election_stage_techspeed for FOJ alignment
-- in the election_stage mart.
with
    -- Roll up gp_api candidacy_stages to election_stage grain. gp_election_id
    -- isn't carried on the candidacy_stage int model, so we recover it via
    -- the candidacy int model (gp_candidacy_id is the join key).
    -- created_at / updated_at use min/max of contributing candidacy_stage
    -- timestamps so gp_api-only rows have a non-null value at the mart layer
    -- (the mart's coalesce will still prefer BR/TS/DDHQ when present).
    gp_api_stages as (
        select
            cs.gp_election_stage_id,
            max(c.gp_election_id) as gp_election_id,
            min(cs.created_at) as created_at,
            max(cs.updated_at) as updated_at
        from {{ ref("int__civics_candidacy_stage_gp_api") }} as cs
        inner join
            {{ ref("int__civics_candidacy_gp_api") }} as c
            on cs.gp_candidacy_id = c.gp_candidacy_id
        where cs.gp_election_stage_id is not null
        group by cs.gp_election_stage_id
    ),

    -- BR FK lookup. gp_api stages anchored to BR's spine adopt BR's
    -- gp_election_stage_id, so this join recovers br_race_id and
    -- br_position_id when the stage is BR-anchored. Non-BR-cluster
    -- canonical_gp_election_stage_ids won't match BR (returning null
    -- on these FK columns — acceptable, since those stages exist
    -- purely from gp_api participation).
    br_fk_lookup as (
        select gp_election_stage_id, br_race_id, br_position_id
        from {{ ref("int__civics_election_stage_ballotready") }}
    )

select
    g.gp_election_stage_id,
    g.gp_election_id,
    cast(br.br_race_id as string) as br_race_id,
    cast(null as string) as br_election_id,
    br.br_position_id,
    cast(null as string) as ddhq_race_id,
    cast(null as string) as stage_type,
    cast(null as date) as election_date,
    cast(null as string) as election_name,
    cast(null as string) as race_name,
    cast(null as boolean) as is_primary,
    cast(null as boolean) as is_runoff,
    cast(null as boolean) as is_retention,
    cast(null as int) as number_of_seats,
    cast(null as string) as total_votes_cast,
    cast(null as string) as partisan_type,
    cast(null as date) as filing_period_start_on,
    cast(null as date) as filing_period_end_on,
    cast(null as string) as filing_requirements,
    cast(null as string) as filing_address,
    cast(null as string) as filing_phone,
    g.created_at,
    g.updated_at
from gp_api_stages as g
left join br_fk_lookup as br on g.gp_election_stage_id = br.gp_election_stage_id
