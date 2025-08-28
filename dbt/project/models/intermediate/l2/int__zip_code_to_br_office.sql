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
    -- ensure the zip code matches with the correct state. In L2 voter data, some
    -- voters have
    -- an out of state zip code and those need to be filtered out.
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
            tbl_zip.exploded_district_name as district_name,
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
            {{ ref("stg_model_predictions__llm_l2_br_match_20250811") }} as tbl_match
            on lower(tbl_zip.exploded_district_name) = lower(tbl_match.l2_district_name)
            and lower(tbl_zip.district_type) = lower(tbl_match.l2_district_type)
            and lower(tbl_zip.state_postal_code) = lower(tbl_match.state)
        left join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as tbl_br_position
            on tbl_match.br_database_id = tbl_br_position.database_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_race") }} as tbl_br_race
            on tbl_br_position.database_id = tbl_br_race.position.databaseid
        -- dedup over the latest race in the database so as not to get a past one
        qualify
            row_number() over (
                partition by zip_code, district_type, district_name, br_race_database_id
                order by br_race_database_id desc
            )
            = 1
    )
/*
the results an be aggregated into a list in multiple ways:
1. Each row can have a zip code and a list of related br_office_ids/race by state and district_type. The code would be like:
```sql
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
```
2. Each row can have a br_office_ids/race with a list of zip codes by state and district_type
*/
select
    zip_code,
    state_postal_code,
    district_type,
    district_name,
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
