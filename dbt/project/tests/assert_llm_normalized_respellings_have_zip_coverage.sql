-- The normalized join arm is what keeps a respelled district's offices visible in
-- zip browsing; narrowing or removing it silently reverts them to invisible. A
-- violation is a matched office whose stored label has no exact in-range zip row
-- but resolves through exactly one map spelling that does have in-range rows, yet
-- is absent from the model. Thins out as stored labels refresh to current
-- spellings. Statewide rows are out of scope (their own mtfcc/retention arm
-- governs them); override-redirected offices are the override path's to cover.
with
    llm_matches as (
        select
            br_database_id,
            l2_state,
            l2_district_type,
            l2_district_name,
            {{ normalize_l2_district_name("l2_district_name") }}
            as normalized_district_name
        from {{ ref("stg_model_predictions__llm_l2_br_match") }}
        where is_matched
    ),
    -- Mirrors the model's active_overrides: rows the LLM did not already place at
    -- the same district redirect their office onto the override path.
    active_overrides as (
        select tbl_override.br_database_id
        from {{ ref("l2_br_match_overrides") }} as tbl_override
        left join
            llm_matches as llm
            on llm.br_database_id = tbl_override.br_database_id
            and lower(llm.l2_district_type) = lower(tbl_override.l2_district_type)
            and lower(llm.l2_district_name) = lower(tbl_override.l2_district_name)
        where llm.br_database_id is null
    ),
    in_range_map as (
        select distinct
            tbl_zip.state_postal_code,
            tbl_zip.district_type,
            tbl_zip.district_name,
            {{ normalize_l2_district_name("tbl_zip.district_name") }}
            as normalized_district_name
        from {{ ref("int__zip_code_to_l2_district") }} as tbl_zip
        inner join
            {{ ref("int__general_states_zip_code_range") }} as zip_range
            on tbl_zip.state_postal_code = zip_range.state_postal_code
            and tbl_zip.zip_code >= zip_range.zip_code_range[0]
            and tbl_zip.zip_code <= zip_range.zip_code_range[1]
    ),
    -- The same shared macro as the model's map_key_spellings, so the two
    -- cannot drift: the whole map, not just in-range.
    map_keys as (
        {{ l2_normalized_district_keys(ref("int__zip_code_to_l2_district")) }}
    ),
    respelled_with_expected_coverage as (
        select llm_matches.br_database_id
        from llm_matches
        inner join
            {{ ref("stg_airbyte_source__ballotready_api_position") }} as office
            on llm_matches.br_database_id = office.database_id
        left join
            in_range_map as exact_map
            on lower(llm_matches.l2_state) = lower(exact_map.state_postal_code)
            and lower(llm_matches.l2_district_type) = lower(exact_map.district_type)
            and lower(llm_matches.l2_district_name) = lower(exact_map.district_name)
        inner join
            map_keys
            on lower(llm_matches.l2_state) = lower(map_keys.state_postal_code)
            and lower(llm_matches.l2_district_type) = lower(map_keys.district_type)
            and llm_matches.normalized_district_name = map_keys.normalized_district_name
            and map_keys.spellings = 1
        inner join
            in_range_map as current_map
            on lower(llm_matches.l2_state) = lower(current_map.state_postal_code)
            and lower(llm_matches.l2_district_type) = lower(current_map.district_type)
            and llm_matches.normalized_district_name
            = current_map.normalized_district_name
        where
            lower(llm_matches.l2_district_type) != 'state'
            and exact_map.district_name is null
            and llm_matches.br_database_id not in (
                select br_database_id
                from active_overrides
                where br_database_id is not null
            )
    )
select distinct respelled_with_expected_coverage.br_database_id
from respelled_with_expected_coverage
left join
    {{ ref("int__zip_code_to_br_office") }} as zip_to_office
    on respelled_with_expected_coverage.br_database_id = zip_to_office.br_database_id
where zip_to_office.br_database_id is null
