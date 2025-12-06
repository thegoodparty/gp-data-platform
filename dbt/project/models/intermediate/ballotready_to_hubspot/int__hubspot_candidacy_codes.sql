{{ config(auto_liquid_cluster=true, tags=["intermediate", "hubspot"]) }}

-- get the unique candidacies from the hubspot data
with
    candidacies_with_codes as (
        select
            {{
                generate_candidate_code(
                    "properties_firstname",
                    "properties_lastname",
                    "properties_state",
                    "properties_office_type",
                    "properties_city",
                )
            }} as hubspot_candidate_code,
            id as hubspot_contact_id,
            updated_at,
            properties_firstname,
            properties_lastname,
            properties_state,
            properties_office_type,
            properties_city
        from {{ ref("int__hubspot_ytd_candidacies") }}
        where
            properties_firstname is not null
            and properties_lastname is not null
            and properties_state is not null
            and properties_office_type is not null
            and properties_city is not null
    )

select *
from candidacies_with_codes
where
    -- filter out cases where hubspot_candidate_code has fields
    -- that filter out to empty strings. Note that underscores (_) must be escaped (\)
    /*
        TOCOMMENT: Should we be keeping records where *any* field is populated,
        or records where *all* fields are populated? The logic of this model vs.
        int__hubspot_candidate_codes differs
    */
    hubspot_candidate_code not ilike '%\_\_\_\_%'
