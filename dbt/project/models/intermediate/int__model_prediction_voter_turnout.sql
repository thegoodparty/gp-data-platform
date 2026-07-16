{{
    config(
        materialized="table",
        on_schema_change="fail",
        auto_liquid_cluster=True,
    )
}}

-- Full-refresh table (DATA-2015): the LightGBM model is authoritative for every
-- natural key it emits; the legacy static feeds supply everything else
-- (Consolidated_General, pre-2026) via the anti-join carve below.
-- It is materialized as a table (not incremental) because the carve only determines
-- the row set on a full rebuild — an incremental merge keyed on model_version would
-- never delete superseded legacy rows.
with
    odd_year_projections as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__turnout_projections_model2odd") }}
        where election_year not in (2027)
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code,
                    model_version
                order by inference_at desc
            )
            = 1
    ),
    even_year_projections_pre_2026 as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__turnout_projections_even_years_20250709") }}
        where election_year < 2026
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code,
                    model_version
                order by inference_at desc
            )
            = 1
    ),
    projections_2026_v2 as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__ballots_projected_2026_v2") }}
        where election_year = 2026
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code,
                    model_version
                order by inference_at desc
            )
            = 1
    ),
    projections_2027 as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__ballots_projected_2027") }}
        where election_year = 2027
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code,
                    model_version
                order by inference_at desc
            )
            = 1
    ),
    projections_2028 as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__ballots_projected_2028") }}
        where election_year = 2028
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code,
                    model_version
                order by inference_at desc
            )
            = 1
    ),
    lgbm_turnout_projections as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at,
            ballots_projected_lower,
            ballots_projected_upper
        from {{ ref("int__voter_turnout_lgbm_inference") }}
        qualify
            row_number() over (
                partition by
                    state, district_type, district_name, election_year, election_code
                order by inference_at desc
            )
            = 1
    ),
    legacy_union as (
        -- Legacy static feeds carry no prediction interval; emit NULL bounds so the
        -- column set and ordering match the lgbm projections for the position-based
        -- union all below (appended last on both sides to keep positions aligned).
        select
            legacy_projections.*,
            cast(null as double) as ballots_projected_lower,
            cast(null as double) as ballots_projected_upper
        from
            (
                select *
                from odd_year_projections
                union all
                select *
                from even_year_projections_pre_2026
                union all
                select *
                from projections_2026_v2
                union all
                select *
                from projections_2027
                union all
                select *
                from projections_2028
            ) as legacy_projections
    )

-- The lgbm model owns every natural key it emits; legacy fills the rest
-- (Consolidated_General, pre-2026). Carving by the FULL natural key (not just
-- year/code) gives the same result as a year/code carve for a full nationwide run,
-- but means a partial/accidental lgbm build can only suppress the exact keys it
-- emitted — never nationwide coverage for a year/code. The all-states coverage test
-- on int__voter_turnout_lgbm_inference enforces fullness in prod.
select *
from lgbm_turnout_projections
union all
select legacy.*
from legacy_union as legacy
where
    not exists (
        select 1
        from lgbm_turnout_projections as m
        where
            m.state = legacy.state
            and m.district_type = legacy.district_type
            and m.district_name = legacy.district_name
            and m.election_year = legacy.election_year
            and m.election_code = legacy.election_code
    )
