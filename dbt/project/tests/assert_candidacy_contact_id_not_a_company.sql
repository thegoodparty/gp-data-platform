-- hubspot_contact_id must reference a HubSpot CONTACT, never a company. gp_api
-- candidacies previously stored the campaign's HubSpot company id here; this
-- test fails on any candidacy whose hubspot_contact_id matches a company id.
select cand.gp_candidacy_id
from {{ ref("candidacy") }} as cand
inner join
    {{ ref("stg_airbyte_source__hubspot_api_companies") }} as co
    on co.id = cand.hubspot_contact_id
