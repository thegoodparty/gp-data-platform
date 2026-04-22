{{ config(tags=["archive"]) }}

-- Historical archive of election stages from elections on or before 2025-12-31.
-- Two parallel grains, unioned:
-- 1) DDHQ-matched: one row per ddhq_race_id. Preserves pre-PR
-- gp_election_stage_id values and distinguishes genuine collisions
-- where two DDHQ races hash to the same gp_election_id (e.g. At-large
-- vs At-large Special, or different position numbers).
-- 2) HubSpot-only: one row per (gp_election_id, stage_type) where no
-- DDHQ race exists at that combo. Irreducible floor for HubSpot-sourced
-- data with no finer discriminator.
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

    -- br_position_id is expected to agree within a (gp_election_id, stage_type);
    -- 99.7% of stages show exactly one distinct value. max() is arbitrary on
    -- disagreement.
    br_position_per_stage as (
        select
            gp_election_id, stage_type, max(br_position_database_id) as br_position_id
        from candidate_stage_implied
        group by gp_election_id, stage_type
    ),

    -- DDHQ-matched path. Grain: one row per ddhq_race_id. gp_election_id and
    -- stage_type are determined by the candidacy match (any_value since they
    -- are race attributes, not candidate-specific). election_date comes from
    -- the DDHQ source of truth.
    ddhq_stages as (
        select
            m.ddhq_race_id,
            any_value(c.gp_election_id) as gp_election_id,
            any_value(
                {{ normalize_ddhq_stage_type("m.ddhq_election_type") }}
            ) as stage_type,
            coalesce(
                any_value(r.election_date), max(c.general_election_date)
            ) as election_stage_date,
            any_value(m.ddhq_race_name) as ddhq_race_name,
            any_value(r.total_number_of_ballots_in_race) as total_votes_cast,
            max(r._airbyte_extracted_at) as _airbyte_extracted_at,
            max(c.updated_at) as src_updated_at
        from {{ ref("int__gp_ai_election_match") }} as m
        inner join
            {{ ref("int__civics_candidacy_2025") }} as c
            on c.gp_candidacy_id = m.gp_candidacy_id
        left join
            {{ ref("stg_airbyte_source__ddhq_gdrive_election_results") }} as r
            on r.ddhq_race_id = m.ddhq_race_id
            and r.candidate_id = m.ddhq_candidate_id
        where m.ddhq_race_id is not null and c.gp_election_id is not null
        group by m.ddhq_race_id
    ),

    -- HubSpot-only path. Covers (gp_election_id, stage_type) combos with no
    -- DDHQ race at that combo. Candidacies whose (gp_election_id, stage_type)
    -- is already represented by a DDHQ race aren't materialized here —
    -- their candidacy_stage FK nulls out via valid_stages in the
    -- candidacy_stage model.
    hubspot_only_stages as (
        select
            s.gp_election_id,
            s.stage_type,
            max(s.stage_date) as election_stage_date,
            max(s.updated_at) as src_updated_at
        from candidate_stage_implied as s
        where
            not exists (
                select 1
                from ddhq_stages d
                where
                    d.gp_election_id = s.gp_election_id and d.stage_type = s.stage_type
            )
        group by s.gp_election_id, s.stage_type
    ),

    combined as (
        select
            {{ generate_salted_uuid(fields=["ddhq_race_id"]) }} as gp_election_stage_id,
            gp_election_id,
            ddhq_race_id,
            stage_type,
            election_stage_date,
            ddhq_race_name,
            total_votes_cast,
            _airbyte_extracted_at,
            src_updated_at
        from ddhq_stages

        union all

        select
            {{ generate_salted_uuid(fields=["gp_election_id", "stage_type"]) }}
            as gp_election_stage_id,
            gp_election_id,
            cast(null as int) as ddhq_race_id,
            stage_type,
            election_stage_date,
            cast(null as string) as ddhq_race_name,
            cast(null as string) as total_votes_cast,
            cast(null as timestamp) as _airbyte_extracted_at,
            src_updated_at
        from hubspot_only_stages
    )

-- Inner join to election_2025 drops stages whose gp_election_id doesn't
-- resolve (residual ghost UUIDs from contest rows with all-null identifying
-- fields).
select
    c.gp_election_stage_id,
    c.gp_election_id,
    c.ddhq_race_id,
    c.stage_type as election_stage,
    c.election_stage_date as ddhq_election_stage_date,
    c.ddhq_race_name,
    bp.br_position_id,
    c.total_votes_cast,
    coalesce(c._airbyte_extracted_at, c.src_updated_at) as created_at
from combined as c
left join
    br_position_per_stage as bp
    on c.gp_election_id = bp.gp_election_id
    and c.stage_type = bp.stage_type
inner join
    {{ ref("int__civics_election_2025") }} as e on c.gp_election_id = e.gp_election_id
