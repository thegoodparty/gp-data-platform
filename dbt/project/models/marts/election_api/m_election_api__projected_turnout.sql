{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
    )
}}

with
    projected_turnout as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "state",
                        "district_type",
                        "district_name",
                    ]
                )
            }} as district_id,
            election_year,
            case
                when election_code = 'Local_or_Municipal'
                then 'LocalOrMunicipal'
                when election_code = 'Consolidated_General'
                then 'ConsolidatedGeneral'
                else election_code
            end as election_code,
            coalesce(ballots_projected, 0) as projected_turnout,
            -- p25 / p95 prediction-interval bounds. NULL for legacy feeds (no
            -- interval); left NULL not coalesced to 0 so a missing bound reads
            -- as "unknown", not "zero turnout".
            ballots_projected_lower as projected_turnout_lower,
            ballots_projected_upper as projected_turnout_upper,
            inference_at,
            model_version
        from {{ ref("int__model_prediction_voter_turnout") }}
    )

-- full rebuild every run: districts that drift out of model coverage drop out
-- instead of stranding stale rows; created_at/updated_at are build timestamps
select
    {{
        generate_salted_uuid(
            fields=[
                "projected_turnout.district_id",
                "projected_turnout.election_year",
                "projected_turnout.election_code",
                "projected_turnout.model_version",
            ]
        )
    }} as id,
    now() as created_at,
    current_timestamp() as updated_at,
    projected_turnout.district_id,
    projected_turnout.election_year,
    projected_turnout.election_code,
    projected_turnout.model_version,
    projected_turnout.projected_turnout,
    projected_turnout.projected_turnout_lower,
    projected_turnout.projected_turnout_upper,
    projected_turnout.inference_at
from projected_turnout
