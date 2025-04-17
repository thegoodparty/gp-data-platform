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
    -- collect all geo_ids from ballotready api place and position tables
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
    -- select distinct geo_ids from geo_ids
    unique_geo_ids as (select distinct geo_id, mtfcc from geo_ids),
    -- determine split indices according to mtfcc and geo_id length
    split_indices as (
        select
            geo_id,
            mtfcc,
            /*
            implement case by case for geo_id length and mtcc according to
            https://docs.google.com/spreadsheets/d/1t1U2oECqFRKPUVO7HYI2me-YzDgtvjbPzp5h9BqKHcU/edit?gid=0#gid=0
            */
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
                when length(geo_id) = 12 and mtfcc not like 'X%'
                then array(2, 5, 11)  -- State + County + Tract + Block Group
                when length(geo_id) = 12 and mtfcc like 'X%'
                then array(2, 5, 7)  -- BallotReady City Council, Justice District, School District
                when length(geo_id) = 15 and mtfcc like 'X%'
                then array(2, 5, 10)  -- BallotReady Township Subdistrict
            end as split_indices
        from unique_geo_ids
    ),
    -- split geo_ids according to split_indices
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
                then geo_id
                when size(split_indices) = 1
                then concat(left(geo_id, split_indices[0]), '-', geo_id)
                when size(split_indices) = 2
                then
                    concat(
                        left(geo_id, split_indices[0]),
                        '-',
                        left(geo_id, split_indices[1]),
                        '-',
                        geo_id
                    )
                when size(split_indices) = 3
                then
                    concat(
                        left(geo_id, split_indices[0]),
                        '-',
                        left(geo_id, split_indices[1]),
                        '-',
                        left(geo_id, split_indices[2]),
                        '-',
                        geo_id
                    )
            end as slug_geo_id_components
        from split_indices
    ),
    with_parent_geo_id_and_place as (
        select
            tbl_geo_id.geo_id,
            tbl_geo_id.mtfcc,
            tbl_geo_id.split_indices,
            tbl_geo_id.split_geo_ids,
            tbl_geo_id.slug_geo_id_components,
            case
                when size(tbl_geo_id.split_geo_ids) >= 1
                then tbl_geo_id.split_geo_ids[size(tbl_geo_id.split_geo_ids) - 1]
                else null
            end as parent_geo_id,
            case
                when len(tbl_place.geo_id) = 2
                then {{ slugify("tbl_place.state") }}
                else {{ slugify("tbl_place.name") }}
            end as place_name_slug
        from splitted_geo_ids as tbl_geo_id
        left join
            {{ ref("stg_airbyte_source__ballotready_api_place") }} as tbl_place
            on tbl_geo_id.geo_id = tbl_place.geo_id
    ),
    geo_id_attributes as (
        select distinct
            geo_id,
            mtfcc,
            slug_geo_id_components,
            parent_geo_id,
            left(geo_id, 2) as state_geo_id,
            coalesce(place_name_slug, '') as place_name_slug
        from with_parent_geo_id_and_place
    ),
    parent_slug_1 as (
        select
            tbl_base.geo_id,
            tbl_base.mtfcc,
            tbl_base.slug_geo_id_components,
            tbl_base.parent_geo_id,
            tbl_base.state_geo_id,
            tbl_base.place_name_slug,
            coalesce(tbl_parent.place_name_slug, '') as parent1_place_name_slug
        from geo_id_attributes as tbl_base
        left join
            geo_id_attributes as tbl_parent
            on tbl_base.parent_geo_id = tbl_parent.geo_id
    ),
    parent_slug_2 as (
        select
            tbl_base.geo_id,
            tbl_base.mtfcc,
            tbl_base.slug_geo_id_components,
            tbl_base.parent_geo_id,
            tbl_base.state_geo_id,
            tbl_base.place_name_slug,
            tbl_base.parent1_place_name_slug,
            coalesce(tbl_parent.parent1_place_name_slug, '') as parent2_place_name_slug
        from parent_slug_1 as tbl_base
        left join
            parent_slug_1 as tbl_parent on tbl_base.parent_geo_id = tbl_parent.geo_id
    ),
    parent_slug_3 as (
        select
            tbl_base.geo_id,
            tbl_base.mtfcc,
            tbl_base.slug_geo_id_components,
            tbl_base.parent_geo_id,
            tbl_base.state_geo_id,
            tbl_base.place_name_slug,
            tbl_base.parent1_place_name_slug,
            tbl_base.parent2_place_name_slug,
            coalesce(tbl_parent.parent2_place_name_slug, '') as parent3_place_name_slug
        from parent_slug_2 as tbl_base
        left join
            parent_slug_2 as tbl_parent on tbl_base.parent_geo_id = tbl_parent.geo_id
    ),
    parent_slug_4 as (
        select
            tbl_base.geo_id,
            tbl_base.mtfcc,
            tbl_base.slug_geo_id_components,
            tbl_base.parent_geo_id,
            tbl_base.state_geo_id,
            tbl_base.place_name_slug,
            tbl_base.parent1_place_name_slug,
            tbl_base.parent2_place_name_slug,
            tbl_base.parent3_place_name_slug,
            coalesce(tbl_parent.parent3_place_name_slug, '') as parent4_place_name_slug
        from parent_slug_3 as tbl_base
        left join
            parent_slug_3 as tbl_parent on tbl_base.parent_geo_id = tbl_parent.geo_id
    ),
    concated_place_name_slugs as (
        select
            geo_id,
            mtfcc,
            slug_geo_id_components,
            parent_geo_id,
            state_geo_id,
            concat(
                parent4_place_name_slug,
                '/',
                parent3_place_name_slug,
                '/',
                parent2_place_name_slug,
                '/',
                parent1_place_name_slug,
                '/',
                place_name_slug
            ) as place_name_slug
        from parent_slug_4
    ),
    final_w_dupes as (
        select
            geo_id,
            mtfcc,
            slug_geo_id_components,
            parent_geo_id,
            state_geo_id,
            {{ slugify("place_name_slug") }} as place_name_slug
        from concated_place_name_slugs
        -- In 660/106k rows, public school districts share the geo_id and mtfcc;
        -- remove by slug length
        qualify
            row_number() over (partition by geo_id, mtfcc order by len(place_name_slug))
            = 1
    ),
    dupes as (
        select place_name_slug, count(*) as dupe_count
        from final_w_dupes
        group by place_name_slug
        having dupe_count > 1
    ),
    deduped as (
        select
            geo_id,
            mtfcc,
            slug_geo_id_components,
            parent_geo_id,
            state_geo_id,
            dupes.place_name_slug || '-' || geo_id as place_name_slug
        from dupes
        left join final_w_dupes on dupes.place_name_slug = final_w_dupes.place_name_slug
    ),
    final as (
        select *
        from deduped
        union
        select *
        from final_w_dupes
        where place_name_slug not in (select place_name_slug from dupes)
    )

select
    geo_id, mtfcc, slug_geo_id_components, parent_geo_id, state_geo_id, place_name_slug
from final
