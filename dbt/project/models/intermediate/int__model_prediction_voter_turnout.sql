{{
    config(
        materialized="incremental",
        unique_key=[
            "state",
            "district_type",
            "district_name",
            "election_year",
            "election_code",
            "model_version",
        ],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "model_prediction", "voter_turnout"],
    )
}}

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
            election_year != 2026
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
                    election_code,
                    model_version
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
