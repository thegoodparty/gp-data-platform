-- depends_on: {{ ref("hubspot_contact_property_columns") }}
select
    -- airbyte record columns (everything else is a hubspot property,
    -- generated from the hubspot_contact_property_columns seed registry;
    -- to expose another property, add a status=generated row there)
    id,
    companies,
    createdat as created_at,
    updatedat as updated_at
    {%- for prop in hubspot_generated_contact_properties() %}
        ,
        {{
            hubspot_contact_property_expression(
                prop.internal_name, prop.cast_type, prop.true_value, prop.false_value
            )
        }} as {{ prop.column_name }}
    {%- endfor %}
from {{ source("airbyte_source", "hubspot_api_contacts") }}
