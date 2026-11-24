-- depends_on: {{ ref("hubspot_contact_property_columns") }}
-- Plain projection of the never-delete history layer over a frozen Airbyte feed
-- plus Lakeflow; the registry loop that types the properties runs there.
with
    history as (select * from {{ ref("int__hubspot_contacts_history") }}),

    -- Frozen extras: the 2026-11-17 associations, kept until the associations
    -- loader replaces them, so a company unlinked since then still shows. Kept
    -- here rather than in the history table, which stays Lakeflow-pure.
    frozen_associations as (
        select id, companies
        from {{ source("airbyte_source", "hubspot_api_contacts") }}
        where companies is not null and trim(companies) not in ('', '[]')
    )

select
    history.id,
    case
        when history._source = 'lakeflow' and frozen_associations.id is not null
        then
            to_json(
                array_distinct(
                    concat(
                        coalesce(
                            from_json(history.companies, 'array<string>'), array()
                        ),
                        from_json(frozen_associations.companies, 'array<string>')
                    )
                )
            )
        else history.companies
    end as companies,
    history.created_at,
    history.updated_at
    {%- for prop in hubspot_generated_contact_properties() %}
        , {{ prop.column_name }}
    {%- endfor %}
from history
left join frozen_associations on history.id = frozen_associations.id
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
