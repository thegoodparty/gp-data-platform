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
        -- dedup to the latest race per BR position per zip+district
        qualify
            row_number() over (
                partition by
                    zip_code, district_type, district_name, tbl_match.br_database_id
                order by br_race_database_id desc
            )
            = 1
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
from zip_code_to_br_office
