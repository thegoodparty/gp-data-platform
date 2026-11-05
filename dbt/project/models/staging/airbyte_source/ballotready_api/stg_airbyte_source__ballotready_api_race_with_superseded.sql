with

    source as (select * from {{ source("airbyte_source", "ballotready_api_race") }}),

    renamed as (

        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            cast(seats as int) as seats,
            from_json(election, 'struct<databaseId:int,id:string>') as election,
            cast(isrecall as boolean) as is_recall,
            cast(isrunoff as boolean) as is_runoff,
            from_json(position, 'struct<databaseId:int,id:string>') as position,
            to_timestamp(createdat) as created_at,
            cast(isprimary as boolean) as is_primary,
            to_timestamp(updatedat) as updated_at,
            cast(databaseid as int) as database_id,
            cast(isdisabled as boolean) as is_disabled,
            cast(ispartisan as boolean) as is_partisan,
            from_json(
                candidacies, 'array<struct<databaseId:int,id:string>>'
            ) as candidacies,
            cast(isunexpired as boolean) as is_unexpired,
            from_json(
                filingperiods, 'array<struct<databaseId:int,id:string>>'
            ) as filing_periods

        from source

    ),

    -- BallotReady sometimes deletes a race and re-creates it for the same position,
    -- election and stage. The Airbyte stream is incremental, so it never sees the
    -- delete and both copies land here. Spot-checks against the API found the older
    -- copy deleted upstream almost every time, and the newest copy live. Flag all but
    -- one race per stage, keeping one that carries candidacies and then the newest,
    -- and never flag a race with candidacies, since candidacy and result data hang
    -- off it.
    with_candidacy_flag as (
        select *, coalesce(size(candidacies), 0) > 0 as has_candidacies from renamed
    ),

    ranked as (
        select
            *,
            row_number() over stage_group as stage_rank,
            first_value(database_id) over stage_group as kept_database_id
        from with_candidacy_flag
        window
            stage_group as (
                partition by
                    position.databaseid,
                    election.databaseid,
                    is_primary,
                    is_runoff,
                    is_recall,
                    is_unexpired
                order by has_candidacies desc, created_at desc, database_id desc
            )
    )

select
    * except (has_candidacies, stage_rank, kept_database_id),
    stage_rank > 1 and not has_candidacies as is_superseded,
    -- The race kept in its place, for remapping a reference to a superseded race.
    case
        when stage_rank > 1 and not has_candidacies then kept_database_id
    end as superseded_by_database_id
from ranked
