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
            -- Campaign fields
            c.campaign_id,
            c.campaign_slug,
            c.hubspot_id,
            c.user_id,
            lower(c.user_email) as user_email,
            initcap(c.user_first_name) as user_first_name,
            initcap(c.user_last_name) as user_last_name,
            c.user_phone,
            c.user_zip,
            c.user_created_at,
            c.created_at as campaign_created_at,
            c.updated_at as campaign_updated_at,
            c.is_verified,
            c.is_active,
            c.is_pro,
            c.is_demo,
            c.is_pledged,
            c.election_date,
            c.campaign_state,
            c.campaign_office,
            c.campaign_party,
            c.election_level,
            c.ballotready_position_id,
            true as is_win_user,

            -- User fields
            u.is_serve_user,
            u.eo_activated_at,

            -- ICP fields
            icp.icp_office_win,
            icp.icp_office_serve,
            initcap(icp.br_position_name) as ballotready_position_name,
            initcap(icp.l2_district_name) as l2_district_name,
            initcap(icp.l2_district_type) as l2_district_type,
            icp.voter_count,

            -- Election dates (from candidacy)
            initcap(cand.official_office_name) as official_office_name,
            cand.candidacy_result,
            cand.viability_score,
            cand.win_number,
            cand.primary_election_date,
            cand.general_election_date,
            cand.runoff_election_date,

            -- Election results by stage
            pr.election_result as primary_election_result,
            ge.election_result as general_election_result,
            ro.election_result as runoff_election_result

        from {{ ref("campaigns") }} c

        -- User
        left join {{ ref("users") }} u on c.user_id = u.user_id

        -- ICP office flags
        left join
            {{ ref("int__icp_offices") }} icp
            on c.ballotready_position_id = icp.br_database_position_id

        -- Candidacy for election dates
        left join
            {{ ref("candidacy") }} cand on c.campaign_id = cand.product_campaign_id

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
