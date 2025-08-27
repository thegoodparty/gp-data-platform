{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="zip_code",
        tags=["intermediate", "l2", "ballotready", "zip_code"],
    )
}}

with
    zip_code_to_l2_district as (
        select
            zip_code,
            state_postal_code,
            district_type,
            district_names,  -- keep for reference if needed
            explode(district_names) as exploded_district_name,
            loaded_at
        from {{ ref("int__zip_code_to_l2_district") }}
        {% if is_incremental() %}
            where loaded_at > (select max(loaded_at) from {{ this }})
        {% endif %}
    ),
    zip_code_within_state_range as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_names,
            tbl_zip.exploded_district_name,
            tbl_zip.loaded_at,
            zip_range.zip_code_range[0] as zip_code_range_lower_bound,
            zip_range.zip_code_range[1] as zip_code_range_upper_bound
        from zip_code_to_l2_district as tbl_zip
        inner join
            {{ ref("int__states_zip_code_range") }} as zip_range
            on tbl_zip.state_postal_code = zip_range.state_postal_code
            and tbl_zip.zip_code >= zip_range.zip_code_range[0]
            and tbl_zip.zip_code <= zip_range.zip_code_range[1]
    ),
    zip_code_to_br_office as (
        select
            tbl_zip.zip_code,
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.exploded_district_name,
            tbl_zip.loaded_at,
            tbl_match.name,
            tbl_match.br_database_id,
            tbl_br_position.id as br_position_id,
            tbl_match.state,
            tbl_match.l2_district_name,
            tbl_match.l2_district_type,
            tbl_match.is_matched,
            tbl_match.llm_reason,
            tbl_match.confidence,
            tbl_match.embeddings,
            tbl_match.top_embedding_score
        from zip_code_within_state_range as tbl_zip
        left join
            {{ ref("stg_model_predictions__llm_l2_br_match_20250811") }} as tbl_match
            on lower(tbl_zip.exploded_district_name) = lower(tbl_match.l2_district_name)
            and lower(tbl_zip.district_type) = lower(tbl_match.l2_district_type)
            and lower(tbl_zip.state_postal_code) = lower(tbl_match.state)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_match.br_database_id = tbl_br_position.database_id
    )

-- ,
-- collected_zip_code_to_br_office as (
-- select
-- zip_code,
-- state_postal_code,
-- district_type,
-- collect_list(br_database_id) as br_database_ids,
-- collect_list(name) as br_office_names,
-- max(loaded_at) as loaded_at
-- from zip_code_to_br_office
-- group by zip_code, state_postal_code, district_type
-- )
-- TODO:
-- add ballotready position_id
-- add tests
-- select
-- zip_code,
-- state_postal_code,
-- district_type,
-- exploded_district_name,
-- loaded_at,
-- name,
-- br_database_id,
-- br_position_id,
-- state,
-- l2_district_name,
-- l2_district_type,
-- is_matched,
-- llm_reason,
-- confidence,
-- embeddings,
-- top_embedding_score
-- from zip_code_to_br_office
select *
-- from collected_zip_code_to_br_office
from zip_code_within_state_range
