{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="geo_id",
        on_schema_change="fail",
        tags=["intermediate", "ballotready", "geo_id_attributes"],
    )
}}

with
    geo_ids as (
        select distinct geo_id, mtfcc
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        {% if is_incremental() %}
            where geo_id not in (select geo_id from {{ this }})
        {% endif %}
        union
        select distinct geo_id, mtfcc
        from {{ ref("stg_airbyte_source__ballotready_api_position") }}
        {% if is_incremental() %}
            where geo_id not in (select geo_id from {{ this }})
        {% endif %}
    ),
    unique_geo_ids as (select distinct geo_id, mtfcc from geo_ids),
    split_indices as (
        select
            geo_id,
            mtfcc,
            case
                when mtfcc in ('X0025', 'X0030', 'X0031', 'X0033', 'X0037', 'X9000')
                then array()  -- BallotReady with empty parent_id
                when length(geo_id) = 2
                then array()  -- State level, no parents
                when length(geo_id) = 4
                then array(2)  -- State + Congressional district
                when length(geo_id) = 5
                then array(2)  -- State + County/Legislative district
                when length(geo_id) = 7
                then array(2)  -- State + Place/City/School District
                when length(geo_id) = 10
                then array(2, 5)  -- State + County + Subdivision
                when length(geo_id) = 11
                then array(2, 5)  -- State + County + Census tract
                when length(geo_id) = 12 and mtfcc like 'X%'
                then array(2, 5, 11)  -- State + County + Tract + Block Group
                when length(geo_id) = 12 and mtfcc like 'X%'
                then array(2, 5, 7)  -- BallotReady City Council, Justice District, School District
                when length(geo_id) = 15 and mtfcc like 'X%'
                then array(2, 5, 10)  -- BallotReady City Council, Justice District, School District
            end as split_indices
        from unique_geo_ids
    ),
    splitted_geo_ids as (
        select
            geo_id,
            mtfcc,
            split_indices,
            case
                when size(split_indices) = 0
                then array()
                when size(split_indices) = 1
                then array(left(geo_id, split_indices[0]))
                when size(split_indices) = 2
                then
                    array(
                        left(geo_id, split_indices[0]), left(geo_id, split_indices[1])
                    )
                when size(split_indices) = 3
                then
                    array(
                        left(geo_id, split_indices[0]),
                        left(geo_id, split_indices[1]),
                        left(geo_id, split_indices[2])
                    )
            end as split_geo_ids,
            case
                when size(split_indices) = 0
                then concat(substring(geo_id, 1, 2), '-', substring(geo_id, 3))
                when size(split_indices) = 1
                then
                    concat(
                        substring(geo_id, 1, split_indices[0]),
                        '-',
                        substring(geo_id, split_indices[0] + 1)
                    )
                when size(split_indices) = 2
                then
                    concat(
                        substring(geo_id, 1, split_indices[0]),
                        '-',
                        substring(
                            geo_id,
                            split_indices[0] + 1,
                            split_indices[1] - split_indices[0]
                        ),
                        '-',
                        substring(geo_id, split_indices[1] + 1)
                    )
                when size(split_indices) = 3
                then
                    concat(
                        substring(geo_id, 1, split_indices[0]),
                        '-',
                        substring(
                            geo_id,
                            split_indices[0] + 1,
                            split_indices[1] - split_indices[0]
                        ),
                        '-',
                        substring(
                            geo_id,
                            split_indices[1] + 1,
                            split_indices[2] - split_indices[1]
                        ),
                        '-',
                        substring(geo_id, split_indices[2] + 1)
                    )
            end as slug
        from split_indices
    ),
    with_parent_id as (
        select
            geo_id,
            mtfcc,
            split_indices,
            split_geo_ids,
            slug,
            case
                when size(split_geo_ids) >= 1
                then split_geo_ids[size(split_geo_ids) - 1]
                else null
            end as parent_id
        from splitted_geo_ids
    ),
    geo_id_attributes as (
        select distinct geo_id, mtfcc, slug, parent_id, left(geo_id, 2) as state
        from with_parent_id
    )

select *
from geo_id_attributes
