-- DATA-1958: overrides for positions absent from the LLM match snapshot must
-- receive zip->office coverage in int__zip_code_to_br_office (the
-- override_zip_to_br_office injection).
--
-- Without that injection the position resolves a district in
-- m_election_api__position but is invisible to the officepicker / zip lookup
-- (m_election_api__zip_to_position), because that path keys its zip->office
-- linkage off the match snapshot rather than the override seed.
--
-- Scoped to overrides whose L2 district actually has voters mapped in
-- int__zip_code_to_l2_district, so a district with no L2 zip coverage does not
-- produce a false failure. Overrides that DO have a snapshot match row are out
-- of scope: they are served by the match-driven path and are not handled by
-- the injection CTE.
--
-- Returns one row per snapshot-absent override missing zip coverage.
with
    snapshot_absent_overrides as (
        select o.br_database_id, o.state, o.l2_district_type, o.l2_district_name
        from {{ ref("l2_br_match_overrides") }} as o
        where
            o.br_database_id not in (
                select br_database_id
                from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
                where br_database_id is not null
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
from snapshot_absent_overrides as a
inner join
    districts_with_zips as z
    on lower(a.state) = z.state
    and lower(a.l2_district_type) = z.district_type
    and lower(a.l2_district_name) = z.district_name
where a.br_database_id not in (select br_database_id from covered)
