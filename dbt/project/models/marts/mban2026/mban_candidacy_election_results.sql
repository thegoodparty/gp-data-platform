select
    mapping.company_id as hubspot_id,
    mapping.gp_candidacy_id,
    matches.election_date,
    matches.election_type,
    matches.has_match,
    matches.llm_confidence as match_confidence,
    matches.llm_reasoning as match_reasoning,
    matches.ddhq_race_id,
    matches.ddhq_candidate_id
from {{ ref("int__hubspot_companies_w_contacts_2025") }} as mapping
inner join
    {{ ref("int__gp_ai_election_match") }} as matches
    on mapping.gp_candidacy_id = matches.gp_candidacy_id
