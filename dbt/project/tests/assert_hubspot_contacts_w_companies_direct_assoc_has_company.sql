-- Every contact that HubSpot directly associates with a company must resolve a
-- company_id in the model. Failures mean the association was dropped (the old
-- engagement-derived join did this for ~50% of associated contacts).
select m.contact_id
from {{ ref("int__hubspot_contacts_w_companies") }} as m
inner join
    {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as c on c.id = m.contact_id
where
    c.companies is not null
    and trim(c.companies) not in ('', '[]')
    and m.company_id is null
