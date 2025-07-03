{{
    config(
        materialized="incremental",
        unique_key=[
            "state",
            "office_type",
            "office_name",
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
            office_type,
            office_name,
            election_year,
            election_code,
            model_version,
            inference_at
        from {{ ref("stg_model_predictions__turnout_projections_model2odd") }}
        {% if is_incremental() %}
            where inference_at >= (select max(inference_at) from {{ this }})
        {% endif %}
    )

select *
from odd_year_projections
