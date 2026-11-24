-- depends_on: {{ ref("hubspot_contact_property_columns") }}
-- Plain projection of the never-delete history layer over a frozen Airbyte feed
-- plus Lakeflow; the registry loop that types the properties runs there.
select
    id,
    companies,
    created_at,
    updated_at
    {%- for prop in hubspot_generated_contact_properties() %}
        , {{ prop.column_name }}
    {%- endfor %}
from {{ ref("int__hubspot_contacts_history") }}
-- On the resolved row, not the raw JSON: a Lakeflow row has no `properties`, and
-- dsar_not_suppressed lets a NULL identifier pass.
where
    {{
        dsar_first_party_not_suppressed(
            "id",
            "hs_contact_id",
            "email",
            "phone",
        )
    }}
