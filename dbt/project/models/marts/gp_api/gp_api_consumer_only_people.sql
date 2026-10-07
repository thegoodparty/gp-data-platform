{#- A table, so the anti-join against the nationwide voter file and the district
    lookup run once per build instead of on every app query. -#}
{{ config(materialized="table", auto_liquid_cluster=True) }}

{%- set voter_columns = adapter.get_columns_in_relation(ref("gp_api_voters")) -%}
{%- set commercial_columns = (
    adapter.get_columns_in_relation(ref("gp_api_commercial_people"))
    | map(attribute="name")
    | map("lower")
    | list
) -%}
{%- set district_types = get_l2_district_types() | map("lower") | list %}

with
    voters as (select `LALVOTERID` from {{ ref("gp_api_voters") }}),

    -- Anti-joined on the whole voter file rather than one state, so an id taken
    -- from LALVOTERID below can never repeat a voter row's id.
    consumer_only as (
        select
            commercial.*,
            concat(
                commercial.`Residence_Addresses_Zip`,
                '-',
                commercial.`Residence_Addresses_ZipPlus4`
            ) as zip_plus_4
        from {{ ref("gp_api_commercial_people") }} as commercial
        left anti join voters on commercial.`LALVOTERID` = voters.`LALVOTERID`
    ),

    profiles as (select * from {{ ref("int__l2_area_district_profiles") }}),

    -- Every district column comes from one area, so the set stays consistent.
    -- ZIP+4 goes first: placing voters by their own commercial address, it
    -- reproduces their voter-file districts slightly more often than the block.
    area_choice as (
        select
            consumer_only.individual_id,
            coalesce(
                zip4.area_type, block.area_type, block_group.area_type, tract.area_type
            ) as area_type,
            coalesce(
                zip4.area_id, block.area_id, block_group.area_id, tract.area_id
            ) as area_id
        from consumer_only
        left join
            profiles as zip4
            on zip4.area_type = 'zip_plus_4'
            and consumer_only.zip_plus_4 = zip4.area_id
        left join
            profiles as block
            on block.area_type = 'census_block'
            and consumer_only.geocode_full = block.area_id
        left join
            profiles as block_group
            on block_group.area_type = 'census_block_group'
            and left(consumer_only.geocode_full, 12) = block_group.area_id
        left join
            profiles as tract
            on tract.area_type = 'census_tract'
            and left(consumer_only.geocode_full, 11) = tract.area_id
    )

select
    {%- for column in voter_columns %}
        {%- set name = column.name %}
        {%- if name | lower == "id" %}
            {#- generate_salted_uuid hashes a null input to a fixed value, so the
                branch has to be explicit rather than a coalesce. #}
            case
                when consumer_only.`LALVOTERID` is not null
                then
                    {{
                        generate_salted_uuid(
                            fields=["consumer_only.`LALVOTERID`"], salt="l2"
                        )
                    }}
                else
                    {{
                        generate_salted_uuid(
                            fields=["consumer_only.individual_id"],
                            salt="l2_commercial",
                        )
                    }}
            end as id,
        {%- elif name | lower in district_types and name | lower in commercial_columns %}
            {#- The commercial file's own value fills in only where no area
                matched, so it never mixes into an area's set. #}
            cast(
                coalesce(
                    profiles.`{{ name }}`, consumer_only.`{{ name }}`
                ) as {{ column.dtype }}
            ) as `{{ name }}`,
        {%- elif name | lower in district_types %}
            cast(profiles.`{{ name }}` as {{ column.dtype }}) as `{{ name }}`,
        {%- elif name | lower in commercial_columns %}
            cast(consumer_only.`{{ name }}` as {{ column.dtype }}) as `{{ name }}`,
        {%- else %} cast(null as {{ column.dtype }}) as `{{ name }}`,
        {%- endif %}
    {%- endfor %}
    false as `Registered_Voter`,
    area_choice.area_type as `District_Source`,
    consumer_only.individual_id
from consumer_only
inner join area_choice on consumer_only.individual_id = area_choice.individual_id
left join
    profiles
    on area_choice.area_type = profiles.area_type
    and area_choice.area_id = profiles.area_id
