-- Overrides the LLM path can't place must get zip->office coverage in
-- int__zip_code_to_br_office (the override_zip_to_br_office injection),
-- otherwise the position has a district but is invisible to the officepicker.
-- This covers overrides absent from the snapshot and those present but
-- unmatched (is_matched = false); only genuinely matched positions are placed
-- by the LLM path. Scoped to overrides whose L2 district has zips, so
-- districts with no L2 zip coverage do not produce false failures.
with
    llm_unplaceable_overrides as (
        select o.br_database_id, o.state, o.l2_district_type, o.l2_district_name
        from {{ ref("l2_br_match_overrides") }} as o
        where
            o.br_database_id not in (
                select br_database_id
                from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
                where br_database_id is not null and is_matched
            )
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
where a.br_database_id not in (select br_database_id from covered)
