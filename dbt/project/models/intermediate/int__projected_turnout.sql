{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        auto_liquid_cluster=true,
        on_schema_change="fail",
        tags=["intermediate", "voter_turnout", "l2", "incremental"],
    )
}}


with
    voter_turnout as (
        select
            state,
            office_type as l2_district_type,
            office_name as l2_district_name,
            ballots_projected as projected_turnout,
            inference_at,
            election_year,
            election_code,
            model_version,
            geoid
        from {{ ref("int__voter_turnout_geoid") }}
        {% if is_incremental() %}
            where inference_at = (select max(inference_at) from {{ this }})
        {% endif %}
    ),
    cleaned_turnout as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "tbl_voter.state",
                        "tbl_voter.l2_district_type",
                        "tbl_voter.l2_district_name",
                        "tbl_voter.election_year",
                        "tbl_voter.election_code",
                        "tbl_voter.inference_at",
                        "tbl_voter.geoid",
                    ]
                )
            }} as id,
            tbl_voter.state,
            tbl_voter.l2_district_type,
            tbl_voter.l2_district_name,
            tbl_voter.projected_turnout,
            tbl_voter.inference_at,
            tbl_voter.election_year,
            tbl_voter.election_code,
            tbl_voter.model_version,
            tbl_voter.geoid,
            tbl_race.br_position_id
        from voter_turnout as tbl_voter
        left join
            {{ ref("int__enhanced_race") }} as tbl_race
            on tbl_voter.geoid = tbl_race.position_geo_id
    )

select
    tbl_cleaned.id,
    tbl_cleaned.state,
    tbl_cleaned.l2_district_type,
    tbl_cleaned.l2_district_name,
    tbl_cleaned.projected_turnout,
    tbl_cleaned.inference_at,
    tbl_cleaned.election_year,
    tbl_cleaned.election_code,
    tbl_cleaned.model_version,
    tbl_cleaned.br_position_id,
    tbl_cleaned.geoid,
    {% if is_incremental() %} coalesce(tbl_existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    now() as updated_at
from cleaned_turnout as tbl_cleaned
{% if is_incremental() %}
    left join {{ this }} as tbl_existing on cleaned_turnout.id = tbl_existing.id
{% endif %}
