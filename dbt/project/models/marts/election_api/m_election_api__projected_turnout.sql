with
    -- Gated on the same predicate as m_election_api__district's turnout branch.
    -- district_id is a salted hash of the tuple, so a row here for a district the
    -- dimension does not mint is a dangling foreign key: the gate withholds
    -- unadopted proposed districts, and turnout predictions for them would point
    -- at nothing.
    turnout as (
        select *
        from {{ ref("int__model_prediction_voter_turnout") }} as turnout
        where
            {{
                retain_district_row(
                    "turnout.district_type", "turnout.state", "turnout.district_name"
                )
            }}
    ),
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
        from turnout
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
    projected_turnout.inference_at
from projected_turnout
