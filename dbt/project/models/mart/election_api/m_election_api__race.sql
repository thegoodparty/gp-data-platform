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
    created_at,
    updated_at,
    br_hash_id,
    br_database_id,
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
    is_runoff,
    is_primary,
    partisan_type,
    filing_date_start,
    filing_date_end,
    employment_type,
    eligibility_requirements,
    salary,
    sub_area_name,
    sub_area_value,
    frequency,
    place_id
from {{ ref("int__enhanced_race") }}
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
