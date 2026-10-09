{{ config(materialized="table") }}

-- The most common full set of L2 district values (and precinct) among voters in each
-- small area,
-- for placing people who are not in the voter file. Taking the whole set from one
-- group of voters, rather than the most common value per column, keeps the columns
-- consistent with each other (a ward always belongs to its city). Values are the
-- voter file's raw strings, so they match gp_api_voters and the district table.
-- Built only for states with a commercial file.
{% set district_types = get_l2_district_types() + ["Precinct"] %}

with
    commercial_states as (
        select distinct state
        from {{ ref("stg_l2_commercial__l2_commercial_20260912_mi") }}
        where state is not null
    ),

    voters as (
        select
            -- L2 stores the block geocode as a number, dropping leading zeros.
            lpad(
                cast(residence_addresses_complete_census_geocode as string), 15, '0'
            ) as block_geoid,
            -- The suffix arrives as text today, but L2 types it as a number; the pad
            -- keeps the join to the commercial file's suffix if that ever shows.
            concat(
                residence_addresses_zip,
                '-',
                lpad(cast(residence_addresses_zipplus4 as string), 4, '0')
            ) as zip_plus_4,
            -- Blanks are nulled to match how m_people_api__voter serves them.
            named_struct(
                {%- for c in district_types %}
                    '{{ c }}',
                    nullif(cast(`{{ c }}` as string), '')
                    {%- if not loop.last %},{% endif %}
                {%- endfor %}
            ) as districts
        from {{ ref("int__l2_nationwide_uniform") }}
        where state_postal_code in (select state from commercial_states)
    ),

    profiled as (select *, sha2(to_json(districts), 256) as profile_id from voters),

    areas as (
        select 'census_block' as area_type, block_geoid as area_id, profile_id
        from profiled
        where block_geoid is not null
        union all
        select 'census_block_group', left(block_geoid, 12), profile_id
        from profiled
        where block_geoid is not null
        union all
        select 'census_tract', left(block_geoid, 11), profile_id
        from profiled
        where block_geoid is not null
        union all
        select 'zip_plus_4', zip_plus_4, profile_id
        from profiled
        where zip_plus_4 is not null
    ),

    profile_counts as (
        select area_type, area_id, profile_id, count(*) as voters_with_profile
        from areas
        group by area_type, area_id, profile_id
    ),

    modal as (
        select
            *,
            sum(voters_with_profile) over (
                partition by area_type, area_id
            ) as voters_in_area
        from profile_counts
        qualify
            row_number() over (
                partition by area_type, area_id
                order by voters_with_profile desc, profile_id
            )
            = 1
    ),

    profiles as (
        select profile_id, any_value(districts) as districts
        from profiled
        group by profile_id
    )

select
    modal.area_type,
    modal.area_id,
    modal.voters_in_area,
    modal.voters_with_profile,
    modal.voters_with_profile / modal.voters_in_area as profile_share,
    profiles.districts.*
from modal
inner join profiles on modal.profile_id = profiles.profile_id
