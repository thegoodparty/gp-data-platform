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
            inference_at,
            model_version
        from {{ ref("int__model_prediction_voter_turnout") }}
        {% if is_incremental() %}
            where
                inference_at >= coalesce(
                    (select max(inference_at) from {{ this }}), '1900-01-01'::timestamp
                )
        {% endif %}
    )

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
    {% if is_incremental() %} coalesce(existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at,
    projected_turnout.district_id,
    projected_turnout.election_year,
    projected_turnout.election_code,
    projected_turnout.model_version,
    projected_turnout.projected_turnout,
    projected_turnout.inference_at
from projected_turnout
{% if is_incremental() %}
    left join
        {{ this }} as existing
        on {{
            generate_salted_uuid(
                fields=[
                    "projected_turnout.district_id",
                    "projected_turnout.election_year",
                    "projected_turnout.election_code",
                    "projected_turnout.model_version",
                ]
            )
        }} = existing.id
{% endif %}
