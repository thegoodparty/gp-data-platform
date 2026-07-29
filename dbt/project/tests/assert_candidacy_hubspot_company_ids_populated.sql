-- Non-archive candidacies whose HubSpot contact has a company association must
-- carry hubspot_company_ids. Failures mean the mart dropped the association for
-- current candidacies (the since-2026 branch previously read a hard-null column).
select cand.gp_candidacy_id
from {{ ref("candidacy") }} as cand
where
    cand.hubspot_contact_id is not null
    and cand.hubspot_company_ids is null
    and exists (
        select 1
        from {{ ref("int__hubspot_contacts_w_companies") }} as hcw
        where
            hcw.contact_id = cand.hubspot_contact_id
            and hcw.extra_companies is not null
            and size(hcw.extra_companies) > 0
    )
