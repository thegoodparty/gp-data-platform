{{ config(auto_liquid_cluster=true, tags=["intermediate", "hubspot"]) }}

-- get the unique candidacies from the hubspot data
select
    {{
        generate_candidate_code(
            "properties_firstname",
            "properties_lastname",
            "properties_state",
            "properties_city",
            "properties_office_type",
        )
    }} as hs_candidate_code
from {{ ref("int__hubspot_ytd_candidacies") }}
where
    properties_firstname is not null
    and properties_lastname is not null
    and properties_state is not null
    and properties_city is not null
    and properties_office_type is not null
