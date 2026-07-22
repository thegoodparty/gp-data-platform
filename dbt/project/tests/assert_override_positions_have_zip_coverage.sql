-- Overrides the LLM path can't place must get zip->office coverage in
-- int__zip_code_to_br_office (the override_zip_to_br_office injection),
-- otherwise the position has a district but is invisible to the officepicker.
-- This covers overrides absent from the snapshot, those present but unmatched
-- (NOT_MATCHED), and those the LLM matched to a different district than the
-- override specifies. Only positions the LLM matched to the same district are
-- placed by the LLM path. Scoped to overrides whose L2 district has zips, so
-- districts with no L2 zip coverage do not produce false failures.
with
    llm_matched_districts as (
        select
            br_database_id,
            lower(l2_district_type) as l2_district_type,
            lower(l2_district_name) as l2_district_name
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
        where br_database_id is not null and is_matched
    ),
    llm_unplaceable_overrides as (
        select o.br_database_id, o.state, o.l2_district_type, o.l2_district_name
        from {{ ref("l2_br_match_overrides") }} as o
        left join
            llm_matched_districts as llm
            on llm.br_database_id = o.br_database_id
            and llm.l2_district_type = lower(o.l2_district_type)
            and llm.l2_district_name = lower(o.l2_district_name)
        where llm.br_database_id is null
    ),
    districts_with_zips as (
        select distinct
            lower(state_postal_code) as state,
            lower(district_type) as district_type,
            lower(district_name) as district_name
        from {{ ref("int__zip_code_to_l2_district") }}
    ),
    covered as (
        select distinct br_database_id
        from {{ ref("int__zip_code_to_br_office") }}
        where br_database_id is not null
    )
select a.br_database_id
from llm_unplaceable_overrides as a
inner join
    districts_with_zips as z
    on lower(a.state) = z.state
    and lower(a.l2_district_type) = z.district_type
    and lower(a.l2_district_name) = z.district_name
where
    a.br_database_id
    not in (select br_database_id from covered where br_database_id is not null)
