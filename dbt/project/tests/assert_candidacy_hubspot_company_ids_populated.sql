-- A candidacy whose gp_api campaign carries a HubSpot company id must surface it
-- as hubspot_company_ids. gp_api stores the HubSpot COMPANY id on the campaign;
-- the mart must expose it. Failures mean the company id was dropped for current
-- candidacies.
select cand.gp_candidacy_id
from {{ ref("candidacy") }} as cand
where
    (cand.hubspot_company_ids is null or trim(cand.hubspot_company_ids) in ('', '[]'))
    and exists (
        select 1
        from {{ ref("int__civics_candidacy_gp_api") }} as gp
        where
            gp.product_campaign_id = cand.product_campaign_id
            and gp.hubspot_company_ids is not null
            and trim(gp.hubspot_company_ids) not in ('', '[]')
    )
