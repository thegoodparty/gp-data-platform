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
                else election_code
            end as election_code,
            coalesce(ballots_projected, 0) as projected_turnout,
            inference_at,
            model_version
        from {{ ref("int__model_prediction_voter_turnout") }}
        where
            -- Past elections are not served: an out-of-horizon race returns no
            -- projection, so these rows answer nothing.
            election_year >= year(current_date())
            -- The retrained model dropped this category. The rows are frozen
            -- output from a retired run, and the app no longer asks for them.
            and election_code <> 'Consolidated_General'
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
