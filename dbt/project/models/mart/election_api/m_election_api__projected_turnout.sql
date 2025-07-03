{{
    config(
        materialized="incremental",
        unique_key="id",
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "model_prediction", "voter_turnout"],
    )
}}

with
    projectected_turnout as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "state",
                        "office_type",
                        "office_name",
                    ]
                )
            }} as district_id
            -- office_type as l2_district_type,
            -- office_name as l2_district_name,
            election_year,
            case
                when election_code = 'Local_or_Municipal'
                then 'LocalOrMunicipal'
                else election_code
            end as election_code,
            ballots_projected as projected_turnout,
            inference_at,
            model_version
        from {{ ref("int__model_prediction_voter_turnout") }}
        {% if is_incremental() %}
            where inference_at >= (select max(inference_at) from {{ this }})
        {% endif %}
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "state",
                "l2_district_type",
                "l2_district_name",
                "election_year",
                "election_code",
                "model_version",
            ]
        )
    }} as id,
    {% if is_incremental() %} coalesce({{ this }}.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at,
    -- state,
    -- l2_district_type,
    -- l2_district_name,
    district_id,
    election_year,
    election_code,
    model_version,
    projected_turnout,
    inference_at
from projectected_turnout
