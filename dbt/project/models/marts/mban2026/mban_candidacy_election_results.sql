with
    joined as (
        select
            mapping.company_id as hubspot_id,
            mapping.gp_candidacy_id,
            matches.election_date,
            matches.election_type,
            matches.has_match,
            matches.llm_confidence as match_confidence,
            matches.llm_reasoning as match_reasoning,
            matches.ddhq_race_id,
            matches.ddhq_candidate_id,
            row_number() over (
                partition by
                    mapping.company_id, matches.election_date, matches.election_type
                order by matches.has_match desc, matches.llm_confidence desc nulls last
            ) as row_rank
        from {{ ref("int__hubspot_contacts_w_companies") }} as mapping
        inner join
            {{ ref("int__gp_ai_election_match") }} as matches
            on mapping.gp_candidacy_id = matches.gp_candidacy_id
        where mapping.company_id is not null
    )

select
    hubspot_id,
    gp_candidacy_id,
    election_date,
    election_type,
    has_match,
    match_confidence,
    match_reasoning,
    ddhq_race_id,
    ddhq_candidate_id
from joined
where row_rank = 1
