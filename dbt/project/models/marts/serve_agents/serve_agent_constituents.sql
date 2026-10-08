-- Serve product constituents: everyone in serve_agent_voters plus the people in
-- the L2 commercial file who are not in the voter file, under the same
-- de-identified, non-partisan columns. Consumer-only rows fill a column only where
-- the commercial file has an equivalent (districts and precinct by area, place,
-- and a few demographics); everything else, including turnout and Haystaq scores,
-- is null. No name, address line or contact column reaches this mart.
{%- set serve_columns = adapter.get_columns_in_relation(ref("serve_agent_voters")) -%}
{%- set people_columns = (
    adapter.get_columns_in_relation(ref("gp_api_consumer_only_people"))
    | map(attribute="name")
    | map("lower")
    | list
) -%}
{#- Raw L2 names in serve_agent_voters whose value the people table carries under
    its voter-contract name, or derives from the block GEOID. -#}
{%- set renamed = {
    "state_postal_code": "`State`",
    "voters_fips": "`FIPS`",
    "voters_gender": "`Gender`",
    "voters_age": "`Age`",
    "consumerdata_education_of_person": "`Education_Of_Person`",
    "consumerdata_language_code": "`Language_Code`",
    "consumerdata_marital_status": "`Marital_Status`",
    "consumerdatall_veteran": "`Veteran_Status`",
    "consumerdata_presence_of_children_in_hh": "`Presence_Of_Children`",
    "residence_addresses_censustract": "substr(geocode_full, 6, 6)",
    "residence_addresses_censusblockgroup": "substr(geocode_full, 12, 1)",
} %}

select
    {%- for column in serve_columns %} `{{ column.name }}`,{% endfor %}
    true as is_registered_voter,
    'voter_file' as district_source
from {{ ref("serve_agent_voters") }}

union all

select
    {%- for column in serve_columns %}
        {%- set name = column.name | lower %}
        {%- if name == "voter_key" %}
            {#- The voter_key the person had while in the voter file. Otherwise
                the commercial id, prefixed so it can never hash to a voter's key
                whatever the two vendor id formats do. #}
            sha2(
                case
                    when `LALVOTERID` is not null
                    then `LALVOTERID`
                    else concat('commercial:', individual_id)
                end,
                256
            ) as voter_key,
        {%- elif name in renamed %}
            cast({{ renamed[name] }} as {{ column.dtype }}) as `{{ column.name }}`,
        {%- elif name in people_columns %}
            cast(`{{ column.name }}` as {{ column.dtype }}) as `{{ column.name }}`,
        {%- else %} cast(null as {{ column.dtype }}) as `{{ column.name }}`,
        {%- endif %}
    {%- endfor %}
    false as is_registered_voter,
    `District_Source` as district_source
from {{ ref("gp_api_consumer_only_people") }}
