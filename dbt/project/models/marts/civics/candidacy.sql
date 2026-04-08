-- Civics mart candidacy table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
    combined as (
        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            br_position_database_id,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at
        from {{ ref("int__civics_candidacy_2025") }}

        union all

        select
            gp_candidacy_id,
            gp_candidate_id,
            gp_election_id,
            product_campaign_id,
            hubspot_contact_id,
            hubspot_company_ids,
            candidate_id_source,
            party_affiliation,
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,
            office_type,
            candidacy_result,
            is_pledged,
            is_verified,
            verification_status_reason,
            is_partisan,
            primary_election_date,
            primary_runoff_election_date,
            general_election_date,
            general_runoff_election_date,
            br_position_database_id,
            viability_score,
            win_number,
            win_number_model,
            created_at,
            updated_at
        from {{ ref("int__civics_candidacy_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (partition by gp_candidacy_id order by updated_at desc)
            = 1
    )

select
    deduplicated.gp_candidacy_id,
    deduplicated.gp_candidate_id,
    deduplicated.gp_election_id,
    deduplicated.product_campaign_id,
    deduplicated.hubspot_contact_id,
    deduplicated.hubspot_company_ids,
    deduplicated.candidate_id_source,
    deduplicated.party_affiliation,
    deduplicated.is_incumbent,
    deduplicated.is_open_seat,
    deduplicated.candidate_office,
    deduplicated.official_office_name,
    deduplicated.office_level,
    deduplicated.office_type,
    deduplicated.candidacy_result,
    deduplicated.is_pledged,
    deduplicated.is_verified,
    deduplicated.verification_status_reason,
    deduplicated.is_partisan,
    deduplicated.primary_election_date,
    deduplicated.primary_runoff_election_date,
    deduplicated.general_election_date,
    deduplicated.general_runoff_election_date,
    deduplicated.br_position_database_id,
    deduplicated.viability_score,
    deduplicated.win_number,
    deduplicated.win_number_model,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.general_election_date is null
                or deduplicated.general_election_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_office_win
    end as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    case
        when
            icp.icp_win_effective_date is not null
            and (
                deduplicated.general_election_date is null
                or deduplicated.general_election_date < icp.icp_win_effective_date
            )
        then false
        else icp.icp_win_supersize
    end as is_win_supersize_icp,
    deduplicated.created_at,
    deduplicated.updated_at

from deduplicated
left join
    {{ ref("int__icp_offices") }} as icp
    on deduplicated.br_position_database_id = icp.br_database_position_id
