{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        tags=["mart", "election_api", "race"],
    )
}}


select
    id,
    br_hash_id,
    br_database_id,
    is_primary,
    is_runoff,
    created_at,
    updated_at,
    election_date,
    position_slug,
    state_slug,
    `state`,
    position_level,
    normalized_position_name,
    position_name,
    position_description,
    filing_office_address,
    filing_phone_number,
    paperwork_instructions,
    filing_requirements,
    partisan_type,
    employment_type,
    eligibility_requirements,
    salary,
    sub_area_name,
    sub_area_value,
    frequency,
    filing_date_start,
    filing_date_end,
    -- need to add field `place` (how to represent object?)
    -- need to add int `place_id` (database_id?)
    /* try position first. join to fun facts with
    select
        tbl_race.br_database_id,
        tbl_race.position_name,
        tbl_pos_ff.city
    from dbt_hugh.m_election_api__race as tbl_race
    left join dbt_hugh.int__position_fun_facts as tbl_pos_ff
        on tbl_pos_ff.database_id = tbl_race.position_database_id
;
    */
    position_database_id
from {{ ref("int__enhanced_race") }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
