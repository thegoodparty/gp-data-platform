-- depends_on: {{ ref("hubspot_contact_property_columns") }}
-- Reads int__hubspot_contacts_history, not the Airbyte source directly: that
-- table is the never-delete layer over a frozen Airbyte feed plus Lakeflow, so
-- the name's "airbyte" now covers two producers. The registry loop that builds
-- the property columns runs there, which is why this model is a plain projection.
select
    -- airbyte record columns (everything else is a hubspot property, typed by
    -- int__hubspot_contacts_history from the hubspot_contact_property_columns
    -- seed registry; to expose another property, add a status=generated row there)
    id,
    companies,
    created_at,
    updated_at
    {#- the typed property columns, in registry order; the history model's own
        bookkeeping columns (lastmodifieddate, _source) are not published here, so
        the output shape stays what downstream already reads -#}
    {%- for prop in hubspot_generated_contact_properties() %}
        , {{ prop.column_name }}
    {%- endfor %}
from {{ ref("int__hubspot_contacts_history") }}
-- The DSAR predicate runs on the resolved row. The history table holds rows from
-- both producers, and a Lakeflow row has no `properties` blob, so filtering the
-- raw JSON would let a contact suppressed by email or phone but not by HubSpot id
-- through. `email` and `phone` are typed columns on both branches.
where
    {{
        dsar_first_party_not_suppressed(
            "id",
            "hs_contact_id",
            "email",
            "phone",
        )
    }}
