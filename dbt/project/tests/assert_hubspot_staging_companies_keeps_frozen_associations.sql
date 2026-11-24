-- Guard for the frozen-association bridge in stg_airbyte_source__hubspot_api_contacts:
-- every company id in a contact's frozen Airbyte `companies` list must still appear in
-- that contact's published `companies` after Lakeflow takes over the row. Lakeflow
-- carries only `associatedcompanyid`, so without the bridge a multi-company contact
-- collapses to its primary company and downstream loses the rest.
--
-- Set containment, not order: downstream explodes the list and ranks the candidates,
-- so the order is not a contract. Zero rows expected.
with
    staged as (
        select id, from_json(companies, 'array<string>') as staged_companies
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        where companies is not null and trim(companies) not in ('', '[]')
    ),

    frozen as (
        select id, from_json(companies, 'array<string>') as frozen_companies
        from {{ source("airbyte_source", "hubspot_api_contacts") }}
        where companies is not null and trim(companies) not in ('', '[]')
    )

select
    staged.id,
    size(staged.staged_companies) as staged_company_count,
    size(frozen.frozen_companies) as frozen_company_count,
    size(
        array_except(frozen.frozen_companies, staged.staged_companies)
    ) as missing_company_count
from staged
inner join frozen on staged.id = frozen.id
where size(array_except(frozen.frozen_companies, staged.staged_companies)) > 0
