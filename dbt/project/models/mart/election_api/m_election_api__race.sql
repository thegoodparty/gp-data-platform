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
    state,
    position_level,
    regexp_replace(normalized_position_name, '//', '-') as normalized_position_name,
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
    coalesce(place_id_by_pos_geo_id, place_id_most_specific_geo_id) as place_id,
    replace(
        concat(
            coalesce(place_slug_by_pos_geo_id, place_slug_most_specific_geo_id),
            '/',
            {{ slugify("normalized_position_name") }}
        ),
        '-ccd',
        ''
    ) as slug,
    position_names
from {{ ref("int__enhanced_race") }}
where
    coalesce(place_id_by_pos_geo_id, place_id_most_specific_geo_id)
    in (select id from {{ ref("m_election_api__place") }})
    and election_date > current_date()
    {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
