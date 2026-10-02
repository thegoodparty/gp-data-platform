-- The feed is built off candidacies but uploaded as contacts, so its grain has to be
-- the person: a person running for two offices inside one window would otherwise
-- arrive in HubSpot as two contacts.
select gp_candidate_id, count(*) as n_rows
from {{ ref("candidacy_hubspot") }}
group by gp_candidate_id
having count(*) > 1
