{{ config(materialized="table") }}

with
    stage_results as (
        select cs.gp_candidacy_id, es.election_stage, cs.election_result
        from {{ ref("candidacy_stage") }} cs
        inner join
            {{ ref("election_stage") }} es
            on cs.gp_election_stage_id = es.gp_election_stage_id
    ),

    final as (
        select
            -- Candidacy core fields
            cand.gp_candidacy_id,
            cand.product_campaign_id,
            cand.gp_candidate_id,
            cand.gp_election_id,
            cand.hubspot_contact_id,
            cand.br_position_database_id,

            -- Candidate fields
            initcap(candidate.full_name) as candidate_name,
            candidate.state as candidate_state,

            -- Candidacy details
            cand.party_affiliation,
            cand.is_incumbent,
            cand.is_open_seat,
            initcap(cand.official_office_name) as official_office_name,
            cand.office_level,
            cand.candidacy_result,
            cand.is_pledged,
            cand.is_verified,
            cand.is_partisan,
            cand.viability_score,
            cand.win_number,

            -- ICP fields
            icp.icp_office_win,
            icp.icp_office_serve,
            initcap(icp.br_position_name) as ballotready_position_name,
            initcap(icp.l2_district_name) as l2_district_name,
            initcap(icp.l2_district_type) as l2_district_type,
            icp.voter_count,
            icp.judicial as is_judicial,
            icp.appointed as is_appointed,

            -- Election dates
            cand.primary_election_date,
            cand.general_election_date,
            cand.runoff_election_date,

            -- Election results by stage
            pr.election_result as primary_election_result,
            ge.election_result as general_election_result,
            ro.election_result as runoff_election_result

        from {{ ref("candidacy") }} cand

        -- Candidate
        left join
            {{ ref("candidate") }} candidate
            on cand.gp_candidate_id = candidate.gp_candidate_id

        -- ICP office flags
        left join
            {{ ref("int__icp_offices") }} icp
            on cand.br_position_database_id = icp.br_database_position_id

        -- Stage results pivoted
        left join
            stage_results pr
            on cand.gp_candidacy_id = pr.gp_candidacy_id
            and pr.election_stage = 'primary'

        left join
            stage_results ge
            on cand.gp_candidacy_id = ge.gp_candidacy_id
            and ge.election_stage = 'general'

        left join
            stage_results ro
            on cand.gp_candidacy_id = ro.gp_candidacy_id
            and ro.election_stage = 'runoff'

    )

select *
from final
