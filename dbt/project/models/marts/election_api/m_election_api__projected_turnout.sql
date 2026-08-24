with
    turnout_source as (
        select
            state,
            district_type,
            district_name,
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
            -- Past election years are not served: an out-of-horizon race
            -- returns no projection, so these rows answer nothing.
            election_year >= year(current_date())
            -- The retrained model dropped this category. The rows are frozen
            -- output from a retired run, and the app no longer asks for them.
            and election_code <> 'Consolidated_General'
    ),
    modelled_districts as (
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
            election_code,
            projected_turnout,
            inference_at,
            model_version
        from turnout_source
    ),
    -- A proposed district carries no projection of its own, and win number and
    -- voter-contact goal derive from turnout alone, so without a carry-over both
    -- collapse to the -1 sentinel for every campaign on the new map.
    --
    -- Knowingly approximate: the old map's electorate is not the new one, worst
    -- in Utah where the two diverge 68%, and the multipliers pass that error into
    -- the contact goal. A wrong-but-reasonable target beats no target, and this
    -- retires when the projection is rerun on the new districts. Because the
    -- fallback lives here rather than in the app, nothing surfaces the
    -- approximation to a caller, so the caveat travels through support.
    carried_over_districts as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "minted.state_postal_code",
                        "minted.district_type",
                        "minted.district_name",
                    ]
                )
            }} as district_id,
            source.election_year,
            source.election_code,
            source.projected_turnout,
            source.inference_at,
            source.model_version
        from {{ ref("int__l2_proposed_district_aggregations") }} as minted
        inner join
            turnout_source as source
            on source.state = minted.state_postal_code
            and source.district_type = minted.shadowed_district_type
            and source.district_name = minted.district_name
    ),
    projected_turnout as (
        select *
        from modelled_districts
        union all
        select *
        from carried_over_districts
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
