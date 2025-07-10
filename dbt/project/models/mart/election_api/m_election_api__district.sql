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
    districts as (
        select distinct
            state,
            district_type as l2_district_type,
            district_name as l2_district_name,
            max(inference_at) as inference_at
        from {{ ref("int__model_prediction_voter_turnout") }}
        {% if is_incremental() %}
            where
                inference_at >= (
                    select coalesce(max(inference_at), '1900-01-01'::timestamp)
                    from {{ this }}
                )
        {% endif %}
        group by state, district_type, district_name
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "districts.state",
                "districts.l2_district_type",
                "districts.l2_district_name",
            ]
        )
    }} as id,
    {% if is_incremental() %} coalesce(existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at,
    districts.state,
    districts.l2_district_type,
    districts.l2_district_name,
    districts.inference_at
from districts
{% if is_incremental() %}
    left join
        {{ this }} as existing
        on {{
            generate_salted_uuid(
                fields=[
                    "districts.state",
                    "districts.l2_district_type",
                    "districts.l2_district_name",
                ]
            )
        }} = existing.id
{% endif %}
