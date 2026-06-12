{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        auto_liquid_cluster=true,
        on_schema_change="fail",
        unique_key=[
            "zip_code",
            "district_type",
            "district_name",
            "br_database_id",
        ],
        tags=["intermediate", "l2", "ballotready", "zip_code"],
    )
}}

with
    zip_code_to_l2_district as (
        select
            zip_code,
            state_postal_code,
            district_type,
            district_name,
            voters_in_zip_district,
            voters_in_zip,
            loaded_at
        from {{ ref("int__zip_code_to_l2_district") }}
        {% if is_incremental() %}
            where loaded_at > (select max(loaded_at) from {{ this }})
        {% endif %}
    ),
    -- Some L2 voters have an out-of-state zip in the L2 file; filter those.
    zip_code_within_state_range as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            tbl_zip.voters_in_zip_district,
            tbl_zip.voters_in_zip,
            tbl_zip.loaded_at
        from zip_code_to_l2_district as tbl_zip
        inner join
            {{ ref("int__general_states_zip_code_range") }} as zip_range
            on tbl_zip.state_postal_code = zip_range.state_postal_code
            and tbl_zip.zip_code >= zip_range.zip_code_range[0]
            and tbl_zip.zip_code <= zip_range.zip_code_range[1]
    ),
    zip_code_to_br_office as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            tbl_zip.voters_in_zip_district,
            tbl_zip.voters_in_zip,
            tbl_zip.loaded_at,
            tbl_match.name,
            tbl_match.br_database_id,
            tbl_br_race.id as br_race_id,
            tbl_br_race.database_id as br_race_database_id,
            tbl_br_position.id as br_position_id,
            tbl_match.l2_district_name,
            tbl_match.l2_district_type,
            tbl_match.is_matched,
            tbl_match.llm_reason,
            tbl_match.confidence,
            tbl_match.embeddings,
            tbl_match.top_embedding_score
        from zip_code_within_state_range as tbl_zip
        left join
            {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as tbl_match
            on lower(tbl_zip.district_name) = lower(tbl_match.l2_district_name)
            and lower(tbl_zip.district_type) = lower(tbl_match.l2_district_type)
            and lower(tbl_zip.state_postal_code) = lower(tbl_match.state)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_match.br_database_id = tbl_br_position.database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_br_race
            on tbl_br_position.database_id = tbl_br_race.position.databaseid
        -- DATA-1986: the LLM BR Office <-> L2 District matcher uses
        -- l2_district_type='State' as a fallback bucket, so non-statewide
        -- positions also land there (district/circuit/appellate judges,
        -- state-legislative districts, water/community special districts).
        -- Restrict statewide coverage to positions whose BR geography is the
        -- whole state (mtfcc='G4000') and that are contestable (exclude
        -- judicial retention seats). Non-'State' district types are unaffected;
        -- curated statewide exceptions flow through override_zip_to_br_office.
        where
            tbl_zip.district_type <> 'State'
            or (
                tbl_br_position.mtfcc = 'G4000'
                and not coalesce(tbl_br_position.is_retention, false)
            )
        -- dedup to the latest race per BR position per zip+district
        qualify
            row_number() over (
                partition by
                    zip_code, district_type, district_name, tbl_match.br_database_id
                order by br_race_database_id desc
            )
            = 1
    ),

    -- Same zip->office linkage as above, but built from the override seed for
    -- positions absent from the LLM snapshot (the match-driven path keys off
    -- the snapshot, so they would otherwise get zero zip rows). Scoped to
    -- snapshot-absent overrides, so existing overrides are untouched and the
    -- unique key cannot collide. Reads the unfiltered zip->district source so a
    -- new override backfills without a full refresh.
    override_zip_to_br_office as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            tbl_zip.voters_in_zip_district,
            tbl_zip.voters_in_zip,
            tbl_zip.loaded_at,
            tbl_override.br_position_name as name,
            tbl_override.br_database_id,
            tbl_br_race.id as br_race_id,
            tbl_br_race.database_id as br_race_database_id,
            tbl_br_position.id as br_position_id,
            tbl_override.l2_district_name,
            tbl_override.l2_district_type,
            true as is_matched,
            'l2_br_match_overrides seed (no LLM match row)' as llm_reason,
            null as confidence,
            null as embeddings,
            null as top_embedding_score
        from {{ ref("int__zip_code_to_l2_district") }} as tbl_zip
        inner join
            {{ ref("int__general_states_zip_code_range") }} as zip_range
            on tbl_zip.state_postal_code = zip_range.state_postal_code
            and tbl_zip.zip_code >= zip_range.zip_code_range[0]
            and tbl_zip.zip_code <= zip_range.zip_code_range[1]
        inner join
            {{ ref("l2_br_match_overrides") }} as tbl_override
            on lower(tbl_zip.district_name) = lower(tbl_override.l2_district_name)
            and lower(tbl_zip.district_type) = lower(tbl_override.l2_district_type)
            and lower(tbl_zip.state_postal_code) = lower(tbl_override.state)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_override.br_database_id = tbl_br_position.database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_br_race
            on tbl_br_position.database_id = tbl_br_race.position.databaseid
        where
            tbl_override.br_database_id not in (
                select br_database_id
                from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
                where br_database_id is not null
            )
        qualify
            row_number() over (
                partition by
                    tbl_zip.zip_code,
                    tbl_zip.district_type,
                    tbl_zip.district_name,
                    tbl_override.br_database_id
                order by tbl_br_race.database_id desc
            )
            = 1
    ),

    combined as (
        select *
        from zip_code_to_br_office
        union all
        select *
        from override_zip_to_br_office
    )
select
    zip_code,
    state_postal_code,
    district_type,
    district_name,
    voters_in_zip_district,
    voters_in_zip,
    loaded_at,
    name,
    br_database_id,
    br_position_id,
    br_race_id,
    br_race_database_id,
    l2_district_name,
    l2_district_type,
    is_matched,
    llm_reason,
    confidence,
    embeddings,
    top_embedding_score
from combined
-- Keep only rows with a live BR position. This drops both LLM-unmatched rows
-- (br_database_id null) and orphan rows whose br_database_id no longer exists
-- in stg_airbyte_source__ballotready_api_position. Unmatched rows would
-- otherwise produce duplicates on incremental merge (the unique_key includes
-- br_database_id, and ANSI null semantics prevent NULL merge keys from
-- matching existing target rows), and the sole downstream consumer
-- (m_election_api__zip_to_position) filters br_database_id is not null anyway.
where br_position_id is not null
