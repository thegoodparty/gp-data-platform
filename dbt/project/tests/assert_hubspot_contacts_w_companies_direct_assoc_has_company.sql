-- Every contact that HubSpot directly associates with a company must resolve a
-- company_id in the model. Failures mean the association was dropped (the old
-- engagement-derived join did this for ~50% of associated contacts).
--
-- Only judge contact versions the model has processed. The staging side is a
-- live view, so an association Airbyte lands between the model's build and this
-- test (or one the incremental has not reached yet) is source lag, not a drop;
-- it failed a 25-minute CI run on a sync that landed 4 minutes before the test.
select m.contact_id
from {{ ref("int__hubspot_contacts_w_companies") }} as m
inner join
    {{ ref("stg_airbyte_source__hubspot_api_contacts") }} as c on c.id = m.contact_id
where
    c.companies is not null
    and trim(c.companies) not in ('', '[]')
    and m.company_id is null
    and c.updated_at <= m.updated_at
