{{ config(materialized="table") }}

with
    stage_results as (
        select cs.gp_candidacy_id, es.stage_type, cs.election_result
        from {{ ref("candidacy_stage") }} cs
        inner join
            {{ ref("election_stage") }} es
            on cs.gp_election_stage_id = es.gp_election_stage_id
        qualify
            row_number() over (
                partition by cs.gp_candidacy_id, es.stage_type
                order by cs.updated_at desc nulls last
            )
            = 1
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
            cand.office_type,
            cand.candidacy_result,
            cand.is_pledged,
            cand.is_verified,
            cand.is_partisan,
            cand.viability_score,
            cand.win_number,

            -- ICP fields. is_win_icp / is_win_supersize_icp are sourced from
            -- the candidacy mart, which already applies the WIN-ICP date gate
            -- (see macro win_icp_date_gate). Other ICP attributes still come
            -- from int__icp_offices via the join below.
            cand.is_win_icp as icp_office_win,
            icp.icp_office_serve,
            cand.is_win_supersize_icp as icp_win_supersize,
            icp.is_judicial,
            icp.is_appointed,
            initcap(icp.br_position_name) as ballotready_position_name,
            initcap(icp.l2_district_name) as l2_district_name,
            initcap(icp.l2_district_type) as l2_district_type,
            icp.voter_count,

            -- Election dates
            cand.primary_election_date,
            cand.primary_runoff_election_date,
            cand.general_election_date,
            cand.general_runoff_election_date,

            -- Election results by stage
            pr.election_result as primary_election_result,
            ge.election_result as general_election_result,
            pro.election_result as primary_runoff_election_result,
            gro.election_result as general_runoff_election_result

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
            and pr.stage_type = 'primary'

        left join
            stage_results ge
            on cand.gp_candidacy_id = ge.gp_candidacy_id
            and ge.stage_type = 'general'

        left join
            stage_results pro
            on cand.gp_candidacy_id = pro.gp_candidacy_id
            and pro.stage_type = 'primary runoff'

        left join
            stage_results gro
            on cand.gp_candidacy_id = gro.gp_candidacy_id
            and gro.stage_type = 'general runoff'

    )

select *
from final
