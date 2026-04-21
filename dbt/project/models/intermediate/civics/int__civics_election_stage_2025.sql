{{ config(tags=["archive"]) }}

-- Historical archive of election stages from elections on or before 2025-12-31.
-- Grain: one row per (gp_election_id, stage_type).
-- Seeded from candidacy dates so HubSpot-only stages are represented. DDHQ
-- attributes (race_id, race_name, total votes) are left-joined when available.
with
    candidate_stage_implied as (
        select
            gp_election_id,
            br_position_database_id,
            'primary' as stage_type,
            primary_election_date as stage_date,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}
        where
            gp_election_id is not null
            and primary_election_date between '1900-01-01' and '2025-12-31'

        union all

        select
            gp_election_id,
            br_position_database_id,
            'general' as stage_type,
            general_election_date as stage_date,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}
        where
            gp_election_id is not null
            and general_election_date between '1900-01-01' and '2025-12-31'

        union all

        select
            gp_election_id,
            br_position_database_id,
            'general runoff' as stage_type,
            general_runoff_election_date as stage_date,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}
        where
            gp_election_id is not null
            and general_runoff_election_date between '1900-01-01' and '2025-12-31'
    ),

    -- Aggregate candidacy-level attributes up to stage grain. br_position_id
    -- is expected to agree within a (gp_election_id, stage_type); 99.7% of
    -- stages show exactly one distinct value. max() is arbitrary on disagreement.
    stages_agg as (
        select
            gp_election_id,
            stage_type,
            max(stage_date) as election_stage_date,
            max(br_position_database_id) as br_position_id,
            max(updated_at) as src_updated_at
        from candidate_stage_implied
        group by gp_election_id, stage_type
    ),

    -- DDHQ attribute join, aggregated to (gp_election_id, stage_type) grain.
    -- Multiple candidate rows per DDHQ race collapse to one via any_value.
    ddhq_attrs as (
        select
            c.gp_election_id,
            case
                when m.ddhq_election_type = 'runoff'
                then 'general runoff'
                else lower(m.ddhq_election_type)
            end as stage_type,
            any_value(m.ddhq_race_id) as ddhq_race_id,
            any_value(m.ddhq_race_name) as ddhq_race_name,
            any_value(r.total_number_of_ballots_in_race) as total_votes_cast,
            max(r._airbyte_extracted_at) as _airbyte_extracted_at
        from {{ ref("int__gp_ai_election_match") }} as m
        inner join
            {{ ref("int__civics_candidacy_2025") }} as c
            on c.gp_candidacy_id = m.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }} as r
            on r.ddhq_race_id = m.ddhq_race_id
            and r.candidate_id = m.ddhq_candidate_id
        where m.ddhq_race_id is not null and c.gp_election_id is not null
        group by 1, 2
    ),

    joined as (
        select
            s.gp_election_id,
            s.stage_type,
            s.election_stage_date,
            s.br_position_id,
            s.src_updated_at,
            d.ddhq_race_id,
            d.ddhq_race_name,
            d.total_votes_cast,
            d._airbyte_extracted_at
        from stages_agg as s
        left join
            ddhq_attrs as d
            on s.gp_election_id = d.gp_election_id
            and s.stage_type = d.stage_type
        inner join
            {{ ref("int__civics_election_2025") }} as e
            on s.gp_election_id = e.gp_election_id
    )

select
    {{ generate_salted_uuid(fields=["gp_election_id", "stage_type"]) }}
    as gp_election_stage_id,
    gp_election_id,
    ddhq_race_id,
    stage_type as election_stage,
    election_stage_date as ddhq_election_stage_date,
    ddhq_race_name,
    br_position_id,
    total_votes_cast,
    coalesce(_airbyte_extracted_at, src_updated_at) as created_at
from joined
