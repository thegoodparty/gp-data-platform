-- The nationwide L2 voter table plus the people in the L2 commercial file who are
-- not in the voter file, under the voter table's own raw L2 columns, so a reader of
-- int__l2_nationwide_uniform_w_haystaq can switch tables without changing a query.
-- Consumer-only rows fill a column only where the commercial file has an
-- equivalent (names, address, phones, districts and precinct by area, a few
-- demographics); party, turnout, registration and Haystaq columns are null.
{%- set voter_columns = adapter.get_columns_in_relation(
    ref("int__l2_nationwide_uniform_w_haystaq")
) -%}
{%- set people_columns = (
    adapter.get_columns_in_relation(ref("gp_api_consumer_only_people"))
    | map(attribute="name")
    | map("lower")
    | list
) -%}
{#- Raw L2 names whose value the people table carries under its voter-contract
    name (the renames in m_people_api__voter), or derives from the block GEOID. -#}
{%- set renamed = {
    "state_postal_code": "`State`",
    "voters_fips": "`FIPS`",
    "voters_firstname": "`FirstName`",
    "voters_middlename": "`MiddleName`",
    "voters_lastname": "`LastName`",
    "voters_namesuffix": "`NameSuffix`",
    "voters_gender": "`Gender`",
    "voters_age": "`Age`",
    "consumerdata_business_owner": "`Business_Owner`",
    "consumerdata_education_of_person": "`Education_Of_Person`",
    "consumerdata_estimated_income_amount": "`Estimated_Income_Amount`",
    "consumerdata_homeowner_probability_model": "`Homeowner_Probability_Model`",
    "consumerdata_language_code": "`Language_Code`",
    "consumerdata_marital_status": "`Marital_Status`",
    "consumerdata_presence_of_children_in_hh": "`Presence_Of_Children`",
    "consumerdatall_veteran": "`Veteran_Status`",
    "residence_addresses_censustract": "substr(geocode_full, 6, 6)",
    "residence_addresses_censusblockgroup": "substr(geocode_full, 12, 1)",
} %}

select
    {%- for column in voter_columns %} `{{ column.name }}`,{% endfor %}
    true as is_registered_voter,
    'voter_file' as district_source,
    cast(null as string) as individual_id
from {{ ref("int__l2_nationwide_uniform_w_haystaq") }}

union all

select
    {%- for column in voter_columns %}
        {%- set name = column.name | lower %}
        {%- if name in renamed %}
            cast({{ renamed[name] }} as {{ column.dtype }}) as `{{ column.name }}`,
        {%- elif name in people_columns %}
            cast(`{{ column.name }}` as {{ column.dtype }}) as `{{ column.name }}`,
        {%- else %} cast(null as {{ column.dtype }}) as `{{ column.name }}`,
        {%- endif %}
    {%- endfor %}
    false as is_registered_voter,
    `District_Source` as district_source,
    individual_id
from {{ ref("gp_api_consumer_only_people") }}
