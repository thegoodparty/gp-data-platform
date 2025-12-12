{{
    config(
        materialized="view",
        tags=["intermediate", "gp_ai", "candidacies"],
    )
}}

with
    candidacies as (select * from {{ ref("m_general__candidacy_v2") }}),
    candidates as (select * from {{ ref("m_general__candidate_v2") }})

select
    candidacies.gp_candidacy_id,
    candidates.first_name,
    candidates.last_name,
    candidates.state,
    candidacies.candidate_office,
    candidacies.official_office_name,
    candidacies.office_level,
    candidacies.party_affiliation,
    candidacies.primary_election_date,
    candidacies.general_election_date,
    candidacies.runoff_election_date,
    candidacies.created_at,
    candidacies.updated_at
from candidacies
left join candidates on candidacies.gp_candidate_id = candidates.gp_candidate_id
