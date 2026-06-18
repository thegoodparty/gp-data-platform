{{
    config(
        materialized="incremental",
        unique_key=[
            "state",
            "district_type",
            "district_name",
            "election_year",
            "election_code",
        ],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "model_prediction", "voter_turnout"],
    )
}}

-- =============================================================================
-- ABOUT THE CTEs BELOW (odd_year_projections, even_year_projections_pre_2026,
-- projections_2026_v2, projections_2027, projections_2028)
-- =============================================================================
--
-- WHY THESE CTEs STILL EXIST BUT ARE NOW DORMANT:
-- Each CTE filters on:  inference_at >= (SELECT MAX(inference_at) FROM this_table)
-- The source tables for these CTEs (ballots_projected_2027, etc.) were written
-- once, have old inference_at timestamps, and WILL NEVER PRODUCE NEW ROWS.
-- They are kept here only to avoid a full table rebuild that would temporarily
-- drop the historical rows they originally contributed.
--
-- NO DATA IS BEING DELETED OR DROPPED.
-- The rows from these old tables were already merged into this table when they
-- were first written. They remain in this table permanently. Removing a CTE
-- here does NOT remove those rows — it only stops dbt from re-reading a source
-- table that has nothing new to offer.
--
-- THE NEW SOURCE OF TRUTH FOR ALL FUTURE PROJECTIONS:
-- The `turnout_projections` CTE at the bottom of this file reads from
-- model_predictions.ballots_projected, which is written by the LightGBM
-- inference pipeline going forward.
-- =============================================================================

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
        where
            election_year not in (2027)
            {% if is_incremental() %}
                and inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
            {% endif %}
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
        where
            election_year < 2026
            {% if is_incremental() %}
                and inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
            {% endif %}
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
        where
            election_year = 2026
            {% if is_incremental() %}
                and inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
            {% endif %}
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
        where
            election_year = 2027
            {% if is_incremental() %}
                and inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
            {% endif %}
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
        where
            election_year = 2028
            {% if is_incremental() %}
                and inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
            {% endif %}
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
    turnout_projections as (
        select
            ballots_projected,
            state,
            district_type,
            district_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__ballots_projected") }}
        {% if is_incremental() %}
            where
                inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
        {% endif %}
        qualify
            row_number() over (
                partition by
                    state,
                    district_type,
                    district_name,
                    election_year,
                    election_code
                order by inference_at desc
            )
            = 1
    )

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
union all
select *
from turnout_projections
