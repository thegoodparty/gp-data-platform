-- A candidacy whose gp_api campaign carries a HubSpot company id must surface it
-- as hubspot_company_ids. gp_api stores the HubSpot COMPANY id on the campaign;
-- the mart must expose it. Failures mean the company id was dropped for current
-- candidacies.
--
-- Keyed on gp_candidacy_id, the grain the mart's coalesce actually operates on.
-- product_campaign_id is not that grain: the 2025 HubSpot archive reuses
-- campaign ids under different candidacy ids, so matching on it pairs an
-- archive candidacy against an unrelated gp_api one.
--
-- The record-version guard keeps this snapshot-safe. gp-api creates the HubSpot
-- company and only then writes its id back onto the campaign, so a campaign is
-- legitimately company-id-less for a while after it is created. A CI run reads
-- the mart from its own schema and this intermediate from prod, where the
-- scheduled job keeps refreshing it, so the two sides can land on either side of
-- that write-back. Comparing only rows built from the same gp_api record version
-- drops the versions that carry no information about the join, without excluding
-- any row on its own merits.
select cand.gp_candidacy_id
from {{ ref("candidacy") }} as cand
inner join
    {{ ref("int__civics_candidacy_gp_api") }} as gp
    on cand.gp_candidacy_id = gp.gp_candidacy_id
    and cand.updated_at = gp.updated_at
where
    (cand.hubspot_company_ids is null or trim(cand.hubspot_company_ids) in ('', '[]'))
    and gp.hubspot_company_ids is not null
    and trim(gp.hubspot_company_ids) not in ('', '[]')
